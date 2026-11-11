'''derive_tables: one transaction per derived table (drop, rebuild, indexes), so a failed rebuild keeps the old table.'''
import sqlite3
import pytest
from nba import NBAdata


@pytest.fixture
def data(tmp_path, monkeypatch):
    db = tmp_path / 'nba.db'
    c = sqlite3.connect(db)
    c.executescript('''CREATE TABLE teams (team_id, teamAbrv);
                       CREATE TABLE src (x);
                       INSERT INTO src VALUES (1), (2);
                       CREATE TABLE derived AS SELECT 99 AS x;''')
    c.commit()
    c.close()
    monkeypatch.setenv('NBA_DB_PATH', str(db))
    return NBAdata.data()


def use_sql(tmp_path, monkeypatch, sql):
    f = tmp_path / 'derive.sql'
    f.write_text(sql)
    monkeypatch.setattr(NBAdata, 'derived_tables', {'derived': {'file': str(f), 'indexes': [('idx_derived_x', 'x')]}})


def test_rebuild_replaces_table_and_adds_index(data, tmp_path, monkeypatch):
    use_sql(tmp_path, monkeypatch, 'DROP TABLE IF EXISTS derived;\nCREATE TABLE derived AS SELECT x FROM src\n-- no trailing semicolon')
    data.derive_tables()
    assert data.conn.execute('SELECT x FROM derived ORDER BY x').fetchall() == [(1,), (2,)]
    assert data.conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'idx_derived_x'").fetchone()


def test_failed_rebuild_keeps_previous_table(data, tmp_path, monkeypatch):
    use_sql(tmp_path, monkeypatch, 'CREATE TABLE derived AS SELECT no_such_column FROM src;')
    with pytest.raises(sqlite3.OperationalError):
        data.derive_tables()
    assert data.conn.execute('SELECT x FROM derived').fetchall() == [(99,)]
    assert not data.conn.in_transaction


def test_reader_sees_old_table_until_commit(data, tmp_path, monkeypatch):
    # a second connection (the app) must never find the table missing mid-rebuild
    data.conn.execute('PRAGMA journal_mode = WAL')
    reader = sqlite3.connect(data.db)
    seen = []
    def peek(*args):
        seen.append(reader.execute('SELECT count(*) FROM derived').fetchone()[0])
        return 0
    use_sql(tmp_path, monkeypatch, 'CREATE TABLE derived AS SELECT x FROM src;')
    data.conn.set_progress_handler(peek, 1)
    data.derive_tables()
    data.conn.set_progress_handler(None, 0)
    # never 'no such table' (peek would raise): the old one-row table until COMMIT, then the new two-row one
    assert seen[0] == 1 and seen == sorted(seen) and set(seen) <= {1, 2}
    assert reader.execute('SELECT count(*) FROM derived').fetchone()[0] == 2
