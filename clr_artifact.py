import asyncio
import os
import shutil
import sqlite3
import xml.etree.ElementTree as ET


class Util():

    def __init__(self):
        self.loop = asyncio.new_event_loop()

    @staticmethod
    async def run(cmd, sema):
        async with sema:
            proc = await asyncio.create_subprocess_shell(
                cmd,
                stderr=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            return ("", f"{cmd}: {stderr}")
        return (stdout.decode('utf-8'), "")

    def get_files(self, urls, paths, parallel=5, extracts=None):
        sema = asyncio.BoundedSemaphore(parallel)
        tasks = []
        if not extracts:
            extracts = ["" for _ in range(len(urls))]
        for url, path, extract in zip(urls, paths, extracts):
            if os.path.isfile(path):
                continue
            os.makedirs(os.path.dirname(path), exist_ok=True)
            if extract:
                tasks.append(self.loop.create_task(self.run(f"curl -L {url} | {extract.format(path)}", sema)))
            else:
                tasks.append(self.loop.create_task(self.run(f"curl -L {url} -o {path}", sema)))
        return self.loop.run_until_complete(asyncio.wait(tasks))


class ClearRepo():

    _REPO_NAME = 'clear'
    _REPO_BIN_SUBURL = os.path.join(_REPO_NAME, 'x86_64/os')
    _REPO_SRC_SUBURL = os.path.join(_REPO_NAME, 'source/SRPMS')

    class _DB():

        def __init__(self, name, path_prefix, repo_type):
            self.name = name
            self.repo_type = repo_type
            self.path = os.path.join(path_prefix, f"{repo_type}-{name}")
            self.cursor = None

        def __repr__(self):
            return f"DB({self.path}, {self.repo_type}, {self.cursor})"

        def __str__(self):
            return self.path

    def __init__(self, uri, version, cache_path):
        self.uri = uri
        self.version = version
        self.cache_path = cache_path
        self.db_cache_path = os.path.join(self.cache_path, self.version, 'db')
        self.dbs = {
            'bin-filelists': ClearRepo._DB('filelists', self.db_cache_path, 'bin'),
            'bin-other': ClearRepo._DB('other', self.db_cache_path, 'bin'),
            'bin-primary': ClearRepo._DB('primary', self.db_cache_path, 'bin'),
            'src-filelists': ClearRepo._DB('filelists', self.db_cache_path, 'src'),
            'src-other': ClearRepo._DB('other', self.db_cache_path, 'src'),
            'src-primary': ClearRepo._DB('primary', self.db_cache_path, 'src')
        }

        self.downloader = Util()
        self.parallel = len(self.dbs)

    def __repr__(self):
        return f"ClearRepo({self.uri}, {self.version}, {self.cache_path})"

    @staticmethod
    def _process_repo_element(element, attribute, compare=None):
        for data in element:
            content = data.attrib.get(attribute)
            if content is not None:
                if compare and content == compare:
                    return data
                elif not compare:
                    return content

    @staticmethod
    def _get_repo_path_from_repomd(repomd, db):
        xml_tree = ET.parse(repomd)
        for elem in xml_tree.iter():
            db_elem = ClearRepo._process_repo_element(elem, 'type', f"{db}_db")
            if db_elem is not None:
                if db_path := ClearRepo._process_repo_element(db_elem, 'href'):
                    return db_path
        return None

    @staticmethod
    def _get_extraction(path):
        if path.endswith('.xz'):
            return 'xzcat -d > {0}'
        if path.endswith('.zst'):
            return 'zstdcat -d -o {0}'

        raise Exception(f"Don't know extract for {path}")

    def _download_repomds(self):
        repo_file = 'repomd.xml'
        repodata_name = 'repodata'
        repomd_bin_url = os.path.join(self.uri, self.version, self._REPO_BIN_SUBURL, repodata_name, repo_file)
        repomd_src_url = os.path.join(self.uri, self.version, self._REPO_SRC_SUBURL, repodata_name, repo_file)
        repomd_bin_path = os.path.join(self.db_cache_path, f"bin-{repo_file}")
        repomd_src_path = os.path.join(self.db_cache_path, f"src-{repo_file}")
        results = self.downloader.get_files([repomd_bin_url, repomd_src_url],
                                            [repomd_bin_path, repomd_src_path])[0]
        for result in results:
            _, error = result.result()
            if error:
                raise Exception(f"download repomd failure: {error}")

        return repomd_bin_path, repomd_src_path

    def _get_db_uri(self, db, repomd):
        repo_name = 'clear'
        if db.repo_type == 'bin':
            suburl = os.path.join(repo_name, 'x86_64/os')
        else:
            suburl = os.path.join(repo_name, 'source/SRPMS')
        suffix = self._get_repo_path_from_repomd(repomd, db.name)
        if not suffix:
            raise Exception(f"Unable to get {db.name} path in {repomd}")
        return os.path.join(self.uri, self.version, suburl, suffix)

    def load_dbs(self):
        if self.version == 'mash' and os.path.exists(self.db_cache_path):
            # always refresh the mash
            shutil.rmtree(self.db_cache_path)

        missing = []
        for name, db in self.dbs.items():
            try:
                db.cursor = sqlite3.connect(db.path).cursor()
            except Exception:
                missing.append(name)
        if missing:
            repomd_bin_path, repomd_src_path = self._download_repomds()
            uris = []
            paths = []
            extracts = []
            for miss in missing:
                db = self.dbs[miss]
                repomd = eval(f"repomd_{db.repo_type}_path")
                uri = self._get_db_uri(db, repomd)
                if not uri:
                    raise Exception(f"Unable to get uri for {db} using {repomd}")
                uris.append(uri)
                paths.append(db.path)
                extracts.append(self._get_extraction(uri))

        results = self.downloader.get_files(uris, paths, extracts=extracts)[0]
        for result in results:
            _, error = result.result()
            if error:
                raise Exception(f"download db failure: {error}")

        for miss in missing:
            db = self.dbs[miss]
            db.cursor = sqlite3.connect(db.path).cursor()

    def _get_srpm_in_source_db(self, name):
        return self.dbs['src-primary'].cursor.execute('select location_href from packages where name = ?',
                                                      (name,)).fetchone()[0]

    def _get_sub_pkgs_from_pkg(self, pkg):
        srpm = self._get_srpm_in_source_db(pkg)
        res = self.dbs['bin-primary'].cursor.execute('select name from packages where rpm_sourcerpm = ?', (srpm,))
        spkgs = set()
        for i in res:
            spkgs.add(i[0])
        return spkgs

    def _get_pkg_name_from_src_key(self, pkgkey):
        return self.dbs['src-primary'].cursor.execute('select name from packages where pkgKey = ?',
                                                      (pkgkey,)).fetchone()[0]

    def _get_pkg_name_from_bin_key(self, pkgkey):
        srpm = self.dbs['bin-primary'].cursor.execute('select rpm_sourcerpm from packages where pkgKey = ?',
                                                      (pkgkey,)).fetchone()[0]
        return self.dbs['src-primary'].cursor.execute('select name from packages where location_href = ?',
                                                      (srpm,)).fetchone()[0]

    def _get_files_matching_dirname(self, file_path):
        files = []
        query = 'select filelist.dirname, filelist.filenames, packages.pkgid from packages inner join filelist' \
            f" using (pkgkey) where filelist.dirname like '%{file_path}%'"
        res = self.dbs['bin-filelists'].cursor.execute(query)
        for dirname, fnames, pkgid in res:
            for fname in fnames.split('/'):
                files.append((os.path.join(dirname, fname), pkgid))
        return files

    def get_packages_matching_path(self, path):
        files = self._get_files_matching_dirname(path)
        pkgids = []
        for _, pkgid in files:
            pkgids.append(pkgid)
        query = f"select name from packages where pkgid in ({','.join(['?'] * len(pkgids))}) group by name"
        res = self.dbs['bin-primary'].cursor.execute(query, pkgids)
        pkgs = []
        for (i,) in res:
            pkgs.append(i)
        return pkgs

    def get_all_requires(self):
        res = self.dbs['bin-primary'].cursor.execute('select name from requires')
        reqs = set()
        for i in res:
            reqs.add(i[0])
        res = self.dbs['src-primary'].cursor.execute('select name from requires')
        for i in res:
            reqs.add(i[0])
        return reqs

    def get_duplicate_provides(self):
        reqs = self.get_all_requires()
        provides = [x for x in self.dbs['bin-primary'].cursor.execute('select name, pkgKey from provides')]
        bins = self._get_files_matching_dirname('/usr/bin')
        pkgids = []
        pkgid_to_bins = {}
        for path, pkgid in bins:
            pkgids.append(pkgid)
            if bins := pkgid_to_bins.get(pkgid):
                bins.append(path)
            else:
                pkgid_to_bins[pkgid] = [path]
        query = f"select pkgKey, pkgid from packages where pkgid in ({','.join(['?'] * len(pkgids))})"
        res = self.dbs['bin-primary'].cursor.execute(query, pkgids)
        for pkgkey, pkgid in res:
            for fname in pkgid_to_bins[pkgid]:
                provides.append((fname, pkgkey))
        prov_pairs = {}
        dups = {}
        for name, pkgkey in provides:
            # only look at provides that something requires
            # as many provides are otherwise not important
            # for package dependency resolution
            if name in reqs:
                if val := prov_pairs.get(name):
                    if val != pkgkey:
                        pkgname = self._get_pkg_name_from_bin_key(pkgkey)
                        if dup := dups.get(name):
                            dup.add(pkgname)
                        else:
                            dups[name] = set([pkgname])
                            first_pkgname = self._get_pkg_name_from_bin_key(val)
                            dups[name].add(first_pkgname)
                else:
                    prov_pairs[name] = pkgkey
        return dups

    def get_pkg_provides(self, name):
        srpm = self._get_srpm_in_source_db(name)
        query = 'select provides.name from provides join packages' \
            ' on provides.pkgKey = packages.pkgKey' \
            ' where packages.rpm_sourcerpm = ?', (srpm,)
        res = self.dbs['bin-primary'].cursor.execute(query)
        provides = []
        for i in res:
            provides += i
        return provides

    def get_pkgs_by_buildreqs(self, buildreqs):
        query = f"select pkgKey from requires where name in ({','.join(['?'] * len(buildreqs))})"
        res = self.dbs['src-primary'].cursor.execute(query, buildreqs)
        pkgkeys = []
        for i in res:
            pkgkeys += i
        pkgs = set()
        for pkgkey in pkgkeys:
            if pkg := self._get_pkg_name_from_src_key(pkgkey):
                pkgs.add(pkg)
        return pkgs

    def _get_pkg_key_from_name(self, pkg):
        return self.dbs['src-primary'].cursor.execute('select pkgkey from packages where name = ?',
                                                      (pkg,)).fetchone()[0]

    def get_pkg_buildreqs(self, pkg):
        key = self._get_pkg_key_from_name(pkg)
        res = self['src-primary'].cursor.execute('select name from requires where pkgKey = ?', (key,))
        reqs = set()
        for i in res:
            reqs.add(i[0])
        return reqs

    def create_graphs(self, pkgs):
        """Take a list of packages and create a set of build order graphs."""
        all_breqs = set()
        pkg_to_breqs = {}
        for pkg in pkgs:
            # build up a map of pkg to its build requirements
            breqs = self.get_pkg_buildreqs(pkg)
            # every build req for all packages being considered
            # is going to be needed later to filter out provides
            # that aren't packages under consideration
            all_breqs = all_breqs.union(breqs)
            pkg_to_breqs[pkg] = breqs

        prvd_to_pkg = {}
        for pkg in pkgs:
            prvds = self.get_pkg_provides(pkg)
            for prvd in prvds:
                # Only use the provide if it is also a build
                # requirement for something else we are using
                if prvd in all_breqs:
                    if spkg := prvd_to_pkg.get(prvd):
                        if spkg != pkg:
                            # This *shouldn't* happen but if it does
                            # at least warn as it probably should be fixed
                            print(f"'WARNING: duplicate provide ({prvd}) for {spkg} and {pkg}")
                    prvd_to_pkg[prvd] = pkg

        bgraph = {}
        dgraph = {}
        for pkg, breqs in pkg_to_breqs.items():
            for breq in breqs:
                if dep := prvd_to_pkg.get(breq):
                    # map pkg to deps
                    if node := bgraph.get(pkg):
                        node.add(dep)
                    else:
                        bgraph[pkg] = set((dep,))
                    # map dep to pkgs
                    if node := dgraph.get(dep):
                        node.add(pkg)
                    else:
                        dgraph[dep] = set((pkg,))
            if not bgraph.get(pkg):
                bgraph[pkg] = set()
        return bgraph, dgraph

    @staticmethod
    def trim_graph(bgraph, dgraph, elements):
        for elem in elements:
            if completed := dgraph.get(elem):
                for to_trim in completed:
                    if node := bgraph.get(to_trim):
                        if elem in node:
                            node.remove(elem)
            if elem in bgraph:
                bgraph.pop(elem)

    @staticmethod
    def break_simple_loops(graph):
        for key, vals in graph.items():
            if key in vals:
                vals.remove(key)
            if len(vals) == 1:
                (val,) = vals
                vitem = graph[val]
                if len(vitem) == 1 and key in vitem:
                    vals.pop()
                    vitem.pop()
