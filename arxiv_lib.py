import csv
import pickle
import re
import click 
from tqdm.auto import tqdm 
from itertools import islice
from arxiv import Client, Search, Result, SortCriterion, SortOrder
from pathlib import Path
from functools import lru_cache
from collections.abc import Iterator, Iterable, Callable
from typing import Pattern, Match, Self, Any
from datetime import datetime
from urllib.request import urlretrieve
from dataclasses import dataclass, field
from dataclass_wizard import Container, JSONListWizard, JSONFileWizard

###################     UTILS     ###################################
THISPATH = Path(__file__).parent

def list_chunk(x: list, size: int) -> Iterator[list]:
    it: Iterator[Any] = iter(x)
    return iter(lambda: list(islice(it, size)), list())

def _mb_int(x: str | None) -> int | None:
            if x is None: return None
            return int(x)

def _pretty_authors_str(authors: tuple[str, ...]) -> str:
    n_authors: int = len(authors)
    authors: tuple[str, ...] = authors[:3]
    def _standartize_author(author: str) -> str:
        parts: list[str] = author.split(' ')
        surname: str = parts[-1].capitalize()
        first_capital_letter: str = parts[0][0].upper()
        return f'{surname}, {first_capital_letter}.'
    authors = tuple(map(_standartize_author, authors))
    if n_authors > 3: authors = authors + ('etc.', )
    return ', '.join(authors)
#####################################################################
###################     STRUCTURES     ##############################
@dataclass(frozen=True)
class PrePrint(JSONListWizard, JSONFileWizard):
    aid: str
    authors: tuple[str, ...]
    title: str
    url: str
    summary: str
    published: datetime
    version: int | None = None

    @classmethod
    @property
    @lru_cache(1)
    def id_pattern(cls) -> Pattern:
        return re.compile(r".*\[?(?P<id>\d{4}\.\d{5})(?:v(?P<ver>\d{1,}))?\]?.*")

    @classmethod
    def from_arxiv_result(cls, r: Result) -> Self:
        parsed_id: Match = cls.id_pattern.fullmatch(r.entry_id)
        return cls(aid=parsed_id.group('id'),
                   authors=tuple(map(lambda a: a.name, r.authors)),
                   title=r.title,
                   url=r.pdf_url,
                   summary=r.summary,
                   published=r.published,
                   version=_mb_int(parsed_id.group('ver')))
    
    @property
    def to_filename(self) -> str:
        name: str = self.aid
        if self.version is not None: name = f'{name}v{self.version}'
        title: str = self.title.replace(':', '.')
        title = title.replace('\t', ' ').replace('/', ' ')
        title = title.replace('\n', '')
        title = title.removesuffix('.')
        authors: str = _pretty_authors_str(self.authors)
        return f'[{name}] {authors} {title}'

#####################################################################
#########################     MAIN     ##############################

class ArxivLib():
    def __init__(self, db_path: Path | str = THISPATH / 'arxiv_db.json', 
                 n_ids_in_request: int = 10) -> None:
        if isinstance(db_path, str): db_path = Path(db_path).expanduser().resolve()
        self._db: dict[str, PrePrint] = dict()
        if db_path.suffix != '.json': raise ValueError('Database should be in JSON format')
        if db_path.exists():
            self._db: Container[PrePrint] = PrePrint.from_json_file(db_path)
            self._db = dict(zip(map(lambda a: a.aid, self._db), self._db))
        self._arxiv_pattern: Pattern = re.compile(
            r"^(?:.*arxiv\.org\/(?:(?:abs)|(?:pdf))\/)?(?P<id>\d{4}\.\d{5})(?:v(?P<ver>\d{1,}))?$"
        )
        self._client: Client = Client()
        self._n_ids_in_request: int = n_ids_in_request
        print(f'{len(self._db)} entries loaded from DB: {db_path}')

    def add_from_old_db(self, db_path: Path | str) -> None:
        if isinstance(db_path, str): db_path = Path(db_path).expanduser().resolve()
        with db_path.open('rb') as db_data: 
            old_db: dict[str, Any] = pickle.load(db_data)
        self.add_by_id(ids=list(old_db.keys()))

    def add_by_url(self, urls: list[str]) -> None:
        def _extract_uid(url: str) -> str | None:
            mb_uid: Match | None = self._arxiv_pattern.fullmatch(url)
            if mb_uid is None: return None
            return mb_uid.group("id")
        self.add_by_id(ids=list(filter(None, map(_extract_uid, urls))))

    def _execute_requests(self, ids: list[str]) -> None:
        chunks: Iterable[Search] = map(lambda chunk: Search(id_list=chunk), 
                                       list_chunk(ids, self._n_ids_in_request))
        preprints: list[PrePrint] = list()
        with tqdm(chunks, total=len(ids), desc='Performing requests...') as pbar:
            for search in pbar:
                n_actual: int = 0
                for res in self._client.results(search):
                    preprints.append(PrePrint.from_arxiv_result(res))
                    n_actual += 1
                pbar.update(n_actual)
        preprints=sorted(preprints, key=lambda pp: pp.published)
        preprints: dict[str, PrePrint] = dict(zip(map(lambda pp: pp.aid, preprints),
                                                      preprints))
        print(f'{len(preprints)} preprints\' meta obtained . DB will updated')
        self._db.update(preprints)

    def _save_db(self, db_path: Path = THISPATH / 'arxiv_db.json') -> None:
        Container[PrePrint](self._db.values()).to_json_file(db_path)
    
    def add_by_id(self, ids: list[str]) -> list[str]:
        n_ids: int = len(ids)
        ids = list(
            filter(
                lambda i: i not in self._db,
                map(lambda m: m.group("id"),
                    filter(None,
                           map(lambda i: PrePrint.id_pattern.fullmatch(i),
                               ids)
                    )
                )
            )
        )
        if not len(ids): return
        print(f'{len(ids)} entries will added to DB')
        if diff := (n_ids - len(ids)): print(f'{diff} entries already known.')
        self._execute_requests(ids=ids)
        self._save_db()
        return ids

    def update_db(self) -> None:
        if not self._db:
            print('DB is empty nothing to update!')
            return
        self._execute_requests(ids=list(self._db.keys()))
        self._save_db()

    def download_pdfs(self, folder: Path | str,
                      ids: list[str] = None,
                      check_exist: bool = True) -> None:
        if isinstance(folder, str): folder = Path(folder).expanduser().resolve()
        folder.mkdir(exist_ok=True, parents=True)
        ex_pdfs: set[str] = set()
        def _parse(x: str) -> tuple[str, int | None] | None:
            parsed: Match = PrePrint.id_pattern.fullmatch(x)
            if parsed is None: return None
            return parsed.group('id'), _mb_int(parsed.group('ver'))
        if check_exist: 
            ex_pdfs = set(filter(None,
                (_parse(p.stem) for p in folder.iterdir() if p.suffix == '.pdf')))
        to_download: list[PrePrint] 
        if ids is not None:
            ids = self.add_by_id(ids=ids)
            to_download = [pp for pp in map(lambda idx: self._db[idx], ids)\
                           if (pp.aid, pp.version) not in ex_pdfs]
        else:
            to_download: list[PrePrint] = [pp for pp in self._db.values()\
                                           if (pp.aid, pp.version) not in ex_pdfs]
        n: int = len(to_download)
        failed: list[str] = list()
        for pp in tqdm(to_download, total=n, desc="Downloading..."):
            name: Path = folder / f'{pp.to_filename}.pdf'
            try: urlretrieve(pp.url, name)
            except: failed.append(pp.aid)
        print(f'Done. {n - len(failed)}/{n} downloaded!')
        if failed: print(f'Details:\n{failed}')

#####################################################################
#########################     CLI     ###############################

@click.command(name='update')
@click.pass_context
def update(ctx: click.Context) -> None:
    ArxivLib(db_path=ctx.obj['db_path'],
             n_ids_in_request=ctx.obj['n_ids_in_request']).update_db()

@click.command(name='download')
@click.option('-s', '--save-path',
              type=click.Path(file_okay=False,
                              resolve_path=True,
                              path_type=Path),
              required=True,
              help='Path where requested PDFs will be saved')
@click.option('-a', '--article',
              type=str, default=None, multiple=True,
              help='ID of article to download. Can accept multiple options'
              )
@click.pass_context
def download(ctx: click.Context,
             save_path: Path,
             article: tuple[str, ...]) -> None:
    lib = ArxivLib(db_path=ctx.obj['db_path'],
                   n_ids_in_request=ctx.obj['n_ids_in_request'])
    if not article: article = None
    else: article = list(article)
    lib.download_pdfs(folder=save_path, ids=article)

@click.command(name='add')
@click.option('-a', '--article',
              type=str, default=None, multiple=True,
              help='ID of article to download. Can accept multiple options'
              )
@click.pass_context
def add(ctx: click.Context,
        ids: list[str]) -> None:
    if not ids: ids = None
    else: ids = list(ids)
    ArxivLib(db_path=ctx.obj['db_path'],
             n_ids_in_request=ctx.obj['n_ids_in_request']).add_by_id(ids=ids)
    pass

@click.command(name='from-Pocket')
@click.option('-f', '--file',
              type=click.Path(exists=True,
                              dir_okay=False,
                              resolve_path=True,
                              path_type=Path),
              required=True,
              help='CSV file obtained from https://getpocket.com/export')
@click.pass_context
def add_from_pocket(ctx: click.Context,
                    file: Path) -> None:
    lib = ArxivLib(db_path=ctx.obj['db_path'],
                   n_ids_in_request=ctx.obj['n_ids_in_request'])
    urls: list[str] = list()
    with file.open('r') as icsv:
        for row in csv.DictReader(icsv): urls.append(row["url"])
    lib.add_by_url(urls)

@click.command(name='from-file')
@click.option('-f', '--file',
              type=click.Path(exists=True,
                              dir_okay=False,
                              resolve_path=True,
                              path_type=Path),
              required=True,
              help='Any plain text file with preprint IDs insidem via separator')
@click.option('--sep', type=str,
              default=',', show_default=True,
              help='IDs separator inside file')
@click.pass_context
def add_from_file(ctx: click.Context,
                  file: Path,
                  sep: str) -> None:
    lib = ArxivLib(db_path=ctx.obj['db_path'],
                   n_ids_in_request=ctx.obj['n_ids_in_request'])
    text: str = file.read_text.rstrip('\n')
    ids: list[str] = list(map(lambda s: s.strip(' '), text.split(sep)))
    lib.add_by_id(ids)

@click.group()
@click.option('-d', '--db-path',
              type=click.Path(dir_okay=False,
                              resolve_path=True,
                              path_type=Path),
              default=THISPATH / 'arxiv_db.json',
              show_default=True,
              help='Path to DB in JSON file.')
@click.option('--n-ids-in-request', type=int,
              default=10,
              show_default=True,
              help="Defines how many preprints ids will be send in single request to arxiv.org\n"\
                   "Number of requests depends on number of preprints devided on this value.!")
@click.pass_context
def cli(ctx: click.Context,
        db_path: Path,
        n_ids_in_request: int) -> None:
    ctx.ensure_object(dict)
    ctx.obj['db_path'] = db_path
    ctx.obj['n_ids_in_request'] = n_ids_in_request
    pass

cli.add_command(update)
cli.add_command(download)
cli.add_command(add)
cli.add_command(add_from_pocket)
cli.add_command(add_from_file)

if __name__ == '__main__':  cli()