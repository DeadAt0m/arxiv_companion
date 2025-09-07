 
# Description:
Collection (since 2018) of preprints from [Arxiv.org](https://arxiv.org) which **I considered necessary to preserve.**

It contains:

1. Infrastructure for hosting Miniflux (RSS feed from `arxiv.org`) + Shiori (to save `arxiv.org` links as bookmarks)
    in **Docker Compose**: `server.yml`
2. Python `tools.py` for articles download/upload from/to Shiori and also downloading pdf from `arxiv.org`
3. Collection in simple json file `arxiv_db.json`, along with simple python script, which helps me form this database.

# Requirements for infrastructure:

1. Docker Compose
2. Fill each `.env` file in `config` folder

# Requirements for python tools:

```bash
 <mamba/conda> env create --name <name> --file conda-env.yml
 <mamba/conda> activate <name>
 ```

`tools.py` self-documented just run:

```bash
python tools.py --help
```
