"""Provision new ephemeral CI databases/roles; never reuse or reset application DBs."""
import os
import secrets
import sys
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main():
    import psycopg
    from psycopg import sql
    from opsyntra.analytics.migrations import migrate
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise ValueError("Provisioning is restricted to disposable GitHub Actions service databases")
    admin_url = os.environ["OPSYNTRA_CI_POSTGRES_ADMIN_URL"]
    github_env = Path(os.environ["GITHUB_ENV"])
    roles = {role:("opsyntra_ci_"+role,secrets.token_hex(24)) for role in ("owner","ingest","api")}
    with psycopg.connect(admin_url,autocommit=True) as c:
        if c.info.dbname != "postgres": raise ValueError("CI administrator must connect to postgres maintenance DB")
        existing = c.execute("SELECT datname FROM pg_database WHERE datname IN ('opsyntra_ci_app','opsyntra_ci_test')").fetchall()
        if existing: raise ValueError("CI databases already exist; refusing to reuse/reset them")
        for name in ("opsyntra_analytics_reader","opsyntra_analytics_writer"):
            c.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(name)))
        for name,password in roles.values():
            c.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(sql.Identifier(name),sql.Literal(password)))
        for name,group in ((roles["ingest"][0],"opsyntra_analytics_writer"),(roles["api"][0],"opsyntra_analytics_reader")):
            c.execute(sql.SQL("GRANT {} TO {}").format(sql.Identifier(group),sql.Identifier(name)))
        for database in ("opsyntra_ci_app","opsyntra_ci_test"):
            c.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(sql.Identifier(database),sql.Identifier(roles["owner"][0])))
            c.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(sql.Identifier(database)))
            for name,_ in roles.values():
                c.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(database),sql.Identifier(name)))
    parsed = urlsplit(admin_url)
    environment = {"OPSYNTRA_RUN_ANALYTICS_TESTS":"1"}
    for scope,database in (("","opsyntra_ci_app"),("TEST_","opsyntra_ci_test")):
        for access,key in (("MIGRATION","owner"),("WRITE","ingest"),("READ","api")):
            name,password = roles[key]
            url = urlunsplit((parsed.scheme,f"{name}:{quote(password)}@{parsed.hostname}:{parsed.port or 5432}","/"+database,"",""))
            environment[f"OPSYNTRA_ANALYTICS_{scope}{access}_URL"] = url
            print("::add-mask::"+url)
        from psycopg.rows import dict_row
        with psycopg.connect(environment[f"OPSYNTRA_ANALYTICS_{scope}MIGRATION_URL"],autocommit=True,row_factory=dict_row) as connection:
            migrate(connection)
    with github_env.open("a",encoding="utf-8") as stream:
        for name,value in environment.items():stream.write(name+"="+value+"\n")
    print("Provisioned two distinct CI databases with separate owner/writer/reader roles")


if __name__ == "__main__":main()
