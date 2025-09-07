import re
import click 
import json
import requests
import operator
from tqdm.auto import tqdm 
from itertools import islice
from pathlib import Path
from functools import lru_cache
from collections.abc import Iterator
from typing import Pattern, Match, Self, Any
from datetime import datetime
from urllib.request import urlretrieve
from dataclasses import dataclass, field
from arxiv import Client, Search, Result
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
#########################     MAIN     ##############################

@dataclass(frozen=True, order=True)
class PrePrint(JSONListWizard, JSONFileWizard):
    aid: str = field(compare=False)
    authors: tuple[str, ...] = field(compare=False)
    title: str = field(compare=False)
    url: str = field(compare=False)
    summary: str = field(compare=False)
    published: datetime
    version: int | None = field(default=None, compare=False)

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
                   title=r.title.replace('\n', ' '),
                   url=r.pdf_url,
                   summary=r.summary.replace('\n', ' '),
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

class ArxivLib():
    def __init__(self, db_path: Path | str = THISPATH / 'arxiv_db.json') -> None:
        if isinstance(db_path, str): db_path = Path(db_path).expanduser().resolve()
        self.db: dict[str, PrePrint] = dict()
        if db_path.suffix != '.json': raise ValueError('Database should be in JSON format')
        if db_path.exists():
            self.db: Container[PrePrint] = PrePrint.from_json_file(db_path)
            self.db = dict(zip(map(lambda a: a.aid, self.db), self.db))
        self._db_path = db_path
        self.client: Client = Client()
        print(f'{len(self.db)} entries loaded from DB: {db_path}')

    def save_db(self, db_path: Path = None) -> None:
        if db_path is None: db_path = self._db_path
        self.db = dict(sorted(self.db.items(), key=operator.itemgetter(1)))
        Container[PrePrint](self.db.values()).to_json_file(db_path)
        print(f'DB Saved. {len(self.db)} pre-prints ')
    
    def download_pdfs(self, folder: Path | str,
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
        to_download: list[PrePrint] = [
            pp for pp in self.db.values()\
            if (pp.aid, pp.version) not in ex_pdfs]
        n: int = len(to_download)
        failed: list[str] = list()
        for pp in (pbar := tqdm(to_download, total=n, desc="Downloading...")):
            name: Path = folder / f'{pp.to_filename}.pdf'
            pbar.set_postfix({'n_failed': len(failed)})
            try: urlretrieve(pp.url, name)
            except: failed.append(pp.to_filename)
        print(f'Done. {n - len(failed)}/{n} downloaded!')
        if failed: print(f'Details:\n{json.dumps(failed, indent=4)}')


#####################################################################
#########################     CLI     ###############################

@click.command(name='dump-shiori')
@click.option('-a', '--address', type=str, required=True,
              help='Address of shiori service')
@click.option('-u', '--user', type=str, required=True,
              help='Shiori user')
@click.option('-p', '--password', type=str, required=True,
              help='Shiori password')
@click.pass_context
def dump_shiori(ctx: click.Context,
                address: str,
                user: str,
                password: str) -> str:
    """Dump all arxiv pre-prints from Shiori into local DB (json)"""
    lib = ArxivLib(db_path=ctx.obj['db_path'])
    address = address.removesuffix('/')
    resp: requests.Responce = requests.post(
            f'{address}/api/v1/auth/login',
            json={"username": user, "password": password, "remember": True, "owner": True})
    resp.raise_for_status()
    sess: str = resp.json()['message']['session']
    resp = requests.get(f'{address}/api/bookmarks', headers={"X-Session-Id": sess})
    resp.raise_for_status()
    npages: int = resp.json()['maxPage']
    for i in tqdm(range(npages), desc='Scaning bookmarks...'):
        r = requests.get(f"{address}/api/bookmarks?keyword=&tags=&exclude=&page={i+1}",
                         headers={"X-Session-Id": sess})
        resp.raise_for_status()
        ids: set[str] = set(
            filter(lambda i: i not in lib.db,
                map(lambda m: m.group('id'),
                    filter(None, map(PrePrint.id_pattern.fullmatch,
                        map(operator.itemgetter('url'),
                            r.json()['bookmarks']))))))
        if not ids: continue
        preprints: list[PrePrint] = list(map(
            PrePrint.from_arxiv_result,
            lib.client.results(Search(id_list=ids, max_results=len(ids)))))
        lib.db.update(zip(map(lambda pp: pp.aid, preprints), preprints))
    lib.save_db()

@click.command(name='upload-shiori')
@click.option('-a', '--address', type=str, required=True,
              help='Address of shiori service')
@click.option('-u', '--user', type=str, required=True,
              help='Shiori user')
@click.option('-p', '--password', type=str, required=True,
              help='Shiori password')
@click.pass_context
def upload_shiori(ctx: click.Context,
                  address: str,
                  user: str,
                  password: str) -> str:
    """Upload all arxiv from local DB (json) to Shiori"""
    lib = ArxivLib(db_path=ctx.obj['db_path'])
    address = address.removesuffix('/')
    resp: requests.Responce = requests.post(
            f'{address}/api/v1/auth/login',
            json={"username": user, "password": password, "remember": True, "owner": True})
    resp.raise_for_status()
    sess: str = resp.json()['message']['session']
    for pp in tqdm(lib.db.values(), total=len(lib.db),
                   desc='Uploading to Shiori...'):
        data: dict[str, Any] = {
            "url": f"https://arxiv.org/abs/{pp.aid}",
	        "createArchive": True,
	        "public": 1,
	        "tags": [],
	        "title": pp.title,
	        "excerpt": pp.summary
        }
        requests.post(
            f"{address}/api/bookmarks",
            headers={"X-Session-Id": sess},
            json=data).raise_for_status()


@click.command(name='download')
@click.option('-s', '--save-path',
              type=click.Path(file_okay=False,
                              resolve_path=True,
                              path_type=Path),
              required=True,
              help='Path where requested PDFs will be saved')
@click.pass_context
def download(ctx: click.Context,
             save_path: Path) -> None:
    """Downloads all articles from DB"""
    ArxivLib(db_path=ctx.obj['db_path']).download_pdfs(folder=save_path)

@click.command(name='info')
@click.pass_context
def info(ctx: click.Context) -> None:
    """Shows number of pre-prints"""
    ArxivLib(db_path=ctx.obj['db_path'])

@click.group()
@click.option('-d', '--db-path',
              type=click.Path(dir_okay=False,
                              resolve_path=True,
                              path_type=Path),
              default=THISPATH / 'arxiv_db.json',
              show_default=True,
              help='Path to DB in JSON file.')
@click.pass_context
def cli(ctx: click.Context,
        db_path: Path) -> None:
    """Tools for managment `arxiv.org` articles inside Shiori"""
    ctx.ensure_object(dict)
    ctx.obj['db_path'] = db_path

cli.add_command(info)
cli.add_command(dump_shiori)
cli.add_command(upload_shiori)
cli.add_command(download)

if __name__ == '__main__':  cli()
