import logging
import lzma
import pickle
import sqlite3
import time
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import NamedTuple, Optional, Union

from evernote.edam.type.ttypes import LinkedNotebook, Note, Notebook

from evernote_backup.config import CURRENT_DB_VERSION, NOTE_CONTENT_KEYWORDS_MAP
from evernote_backup.evernote_types import Reminder, Task
from evernote_backup.log_util import log_format_note, log_format_notebook, log_operation_time

import json
import re


def parse_note_content(content: str) -> dict:
    """从笔记内容中解析ext字段
    
    根据NOTE_CONTENT_KEYWORDS_MAP中定义的关键词，从笔记内容开头提取对应字段。
    笔记内容可能是XML格式（ENML），需要先提取文本内容。
    格式：关键词：内容
    如果匹配多个则取第一个。
    
    Args:
        content: 笔记的文本内容（可能是XML格式）
        
    Returns:
        解析后的字段字典，key为ext字段名，value为提取的内容
    """
    if not content:
        return {}
    
    # 从XML中提取文本内容
    # 移除XML标签，保留文本
    text_content = extract_text_from_xml(content)
    
    if not text_content:
        return {}
    
    result = {}
    
    # 在内容开头查找（限制在前3000字符内，避免扫描整个长文档）
    search_content = text_content[:3000]
    
    # 按行分割内容
    lines = search_content.split('\n')
    
    # 遍历所有关键词映射
    for keyword, field_key in NOTE_CONTENT_KEYWORDS_MAP.items():
        # 查找包含关键词的行
        for i, line in enumerate(lines):
            # 检查行是否以关键词开头
            if re.match(rf'{re.escape(keyword)}[：:]\s*', line):
                # 提取关键词后的内容（在同一行）
                match = re.match(rf'{re.escape(keyword)}[：:]\s*(.*)', line)
                if match:
                    value = match.group(1).strip()
                    
                    # 如果当前行只有关键词没有内容，检查下一行
                    if not value and i + 1 < len(lines):
                        next_line = lines[i + 1].strip()
                        # 下一行不能是另一个关键词
                        if next_line and not any(re.match(rf'{re.escape(k)}[：:]\s*', next_line) for k in NOTE_CONTENT_KEYWORDS_MAP.keys()):
                            value = next_line
                    
                    # 只保存非空内容
                    if value:
                        result[field_key] = value
                        logger.debug(f'Found {keyword}: {value}')
                break
    
    return result


def extract_text_from_xml(xml_content: str) -> str:
    """从XML内容中提取纯文本
    
    处理Evernote的ENML格式，移除所有XML标签，保留文本内容。
    同时处理CDATA部分。
    
    Args:
        xml_content: XML格式的内容
        
    Returns:
        提取的纯文本
    """
    if not xml_content:
        return ""
    
    # 移除XML声明和DOCTYPE
    content = re.sub(r'<\?xml[^?]*\?>', '', xml_content)
    content = re.sub(r'<!DOCTYPE[^>]*>', '', content)
    
    # 移除CDATA标记，保留内容
    content = re.sub(r'<!\[CDATA\[', '', content)
    content = re.sub(r'\]\]>', '', content)
    
    # 移除所有XML标签，保留标签之间的文本
    # 使用非贪婪匹配移除标签
    content = re.sub(r'<[^>]+>', '\n', content)
    
    # 合并多个换行符为单个换行符
    content = re.sub(r'\n+', '\n', content)
    
    # 移除多余的空白字符
    content = re.sub(r'[ \t]+', ' ', content)
    
    return content.strip()

logger = logging.getLogger(__name__)


class NoteForSync(NamedTuple):
    guid: str
    title: str
    linked_notebook_guid: Optional[str]


DB_SCHEMA = """CREATE TABLE IF NOT EXISTS notebooks(
                        guid TEXT PRIMARY KEY,
                        name TEXT,
                        stack TEXT
                    );
                    CREATE TABLE IF NOT EXISTS notebooks_linked(
                        guid TEXT PRIMARY KEY,
                        notebook_guid TEXT,
                        usn INT DEFAULT 0
                    );
                    CREATE TABLE IF NOT EXISTS notes(
                        guid TEXT PRIMARY KEY,
                        title TEXT,
                        notebook_guid TEXT,
                        is_active BOOLEAN,
                        create_time INT DEFAULT 0,
                        update_time INT DEFAULT 0,
                        sync_time INT DEFAULT 0,
                        tag TEXT,
                        ext TEXT,
                        raw_note BLOB
                    );
                    CREATE TABLE IF NOT EXISTS tasks(
                        guid TEXT PRIMARY KEY,
                        note_guid TEXT,
                        raw_task BLOB
                    );
                    CREATE TABLE IF NOT EXISTS reminders(
                        guid TEXT PRIMARY KEY,
                        task_guid TEXT,
                        raw_reminder BLOB
                    );
                    CREATE TABLE IF NOT EXISTS config(
                        name TEXT PRIMARY KEY,
                        value TEXT
                    );
                    CREATE INDEX IF NOT EXISTS idx_notes
                     ON notes(notebook_guid, is_active);
                    CREATE INDEX IF NOT EXISTS idx_notes_title
                     ON notes(title COLLATE NOCASE);
                    CREATE INDEX IF NOT EXISTS idx_notebooks_linked
                     ON notebooks_linked(guid, notebook_guid);
                    CREATE INDEX IF NOT EXISTS idx_notes_raw_null
                     ON notes(guid) WHERE raw_note IS NULL;
                    CREATE INDEX IF NOT EXISTS idx_tasks
                     ON tasks(note_guid);
                    CREATE INDEX IF NOT EXISTS idx_reminders
                     ON reminders(task_guid);
"""


class DatabaseResyncRequiredError(Exception):
    """Raise when database update requires resync"""


def initialize_db(database_path: Path) -> None:
    if database_path.exists():
        raise FileExistsError

    db = sqlite3.connect(database_path)

    with db as con:
        con.executescript(DB_SCHEMA)

    db.close()


class SqliteStorage:
    def __init__(self, database: Union[Path, sqlite3.Connection]) -> None:
        if isinstance(database, sqlite3.Connection):
            self.db = database
        else:
            if not database.exists():
                raise FileNotFoundError("Database file does not exist.")

            self.db = sqlite3.connect(database)
            self.db.row_factory = sqlite3.Row

    @property
    def config(self) -> "ConfigStorage":
        return ConfigStorage(self.db)

    @property
    def notes(self) -> "NoteStorage":
        return NoteStorage(self.db)

    @property
    def notebooks(self) -> "NoteBookStorage":
        return NoteBookStorage(self.db)

    @property
    def tasks(self) -> "TasksStorage":
        return TasksStorage(self.db)

    @property
    def reminders(self) -> "RemindersStorage":
        return RemindersStorage(self.db)

    def integrity_check(self) -> str:
        with self.db as con:
            cur = con.execute("PRAGMA integrity_check;")

            try:
                results = cur.fetchall()
            except sqlite3.Error as e:
                return str(e)

            return "\n".join(row[0] for row in results)

    def check_version(self) -> None:
        try:
            db_version = int(self.config.get_config_value("DB_VERSION"))
        except KeyError:
            db_version = 0

        if db_version != CURRENT_DB_VERSION:
            logger.info(
                f"Upgrading database version from {db_version} to {CURRENT_DB_VERSION}..."
            )
            self.upgrade_db(db_version)

    def upgrade_db(self, db_version: int) -> None:
        need_resync = False

        if db_version == 0:
            need_resync = True
            with self.db as con1:
                con1.execute("DROP TABLE notebooks;")
                con1.execute("DROP TABLE notes;")

                con1.executescript(DB_SCHEMA)

        if db_version < 3:
            with self.db as con2:
                con2.execute(
                    "CREATE INDEX IF NOT EXISTS idx_notes_title"
                    " ON notes(title COLLATE NOCASE);"
                )

        if db_version < 4:
            with self.db as con3:
                con3.execute(
                    "CREATE TABLE IF NOT EXISTS notebooks_linked("
                    " guid TEXT PRIMARY KEY,"
                    " notebook_guid TEXT,"
                    " usn INT DEFAULT 0"
                    " );"
                )
                con3.execute(
                    "CREATE INDEX IF NOT EXISTS idx_notebooks_linked"
                    " ON notebooks_linked(guid, notebook_guid);"
                )

        if db_version < 5:
            with self.db as con4:
                con4.execute(
                    "CREATE INDEX IF NOT EXISTS idx_notes_raw_null"
                    " ON notes(guid) WHERE raw_note IS NULL;"
                )

        if db_version < 6:
            self.config.set_config_value("last_connection_tasks", "0")

            with self.db as con5:
                con5.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS tasks(
                        guid TEXT PRIMARY KEY,
                        note_guid TEXT,
                        raw_task BLOB
                    );
                    CREATE TABLE IF NOT EXISTS reminders(
                        guid TEXT PRIMARY KEY,
                        task_guid TEXT,
                        raw_reminder BLOB
                    );
                    CREATE INDEX IF NOT EXISTS idx_tasks
                     ON tasks(note_guid);
                    CREATE INDEX IF NOT EXISTS idx_reminders
                     ON reminders(task_guid);
                    """
                )

        if db_version < 7:
            with self.db as con:
                cur = con.execute("PRAGMA table_info(notes)")
                columns = [row["name"] for row in cur.fetchall()]

                if "update_time" not in columns:
                    con.execute("ALTER TABLE notes ADD COLUMN update_time INT DEFAULT 0;")

                cur = con.execute("PRAGMA table_info(notes)")
                columns = [row["name"] for row in cur.fetchall()]

                # 版本7的升级逻辑：处理sync_time字段和添加create_time字段
                if "sync_time" not in columns:
                    # 检查是否有旧的 content_update_time 字段
                    if "content_update_time" in columns:
                        logger.info("Migrating content_update_time to sync_time...")
                        
                        # 添加新的 sync_time 字段
                        con.execute("ALTER TABLE notes ADD COLUMN sync_time INT DEFAULT 0;")
                        
                        # 对于大表，使用简单的全量更新，避免复杂的分批逻辑可能陷入死循环
                        logger.info("Starting full migration of content_update_time to sync_time...")
                        
                        # 直接全量更新，SQLite会处理大表的更新
                        result = con.execute("""
                            UPDATE notes 
                            SET sync_time = content_update_time 
                            WHERE content_update_time!=0;
                        """)
                        
                        rows_updated = result.rowcount
                        logger.info(f"Migration completed. Total rows migrated: {rows_updated}")
                        
                        # 提交更新
                        con.commit()
                        
                        # 尝试删除旧的 content_update_time 字段
                        # 注意：对于30GB的大表，这可能失败或很慢
                        try:
                            logger.info("Attempting to drop content_update_time column...")
                            
                            # 检查SQLite版本是否支持DROP COLUMN
                            cur = con.execute("SELECT sqlite_version()")
                            sqlite_version = cur.fetchone()[0]
                            logger.info(f"SQLite version: {sqlite_version}")
                            
                            # SQLite 3.35.0 (2021-03-12) 开始支持DROP COLUMN
                            version_parts = tuple(map(int, sqlite_version.split('.')))
                            if version_parts >= (3, 35, 0):
                                # 使用新的ALTER TABLE DROP COLUMN语法
                                con.execute("ALTER TABLE notes DROP COLUMN content_update_time")
                                logger.info("Successfully dropped content_update_time column")
                            else:
                                logger.warning(
                                    f"SQLite version {sqlite_version} does not support DROP COLUMN. "
                                    "Keeping content_update_time column (unused)."
                                )
                        except Exception as e:
                            logger.warning(
                                f"Failed to drop content_update_time column: {e}. "
                                "Keeping column (unused). This is expected for large tables."
                            )
                    else:
                        # 如果没有旧的 content_update_time 字段，直接添加 sync_time 字段
                        con.execute("ALTER TABLE notes ADD COLUMN sync_time INT DEFAULT 0;")

                # 版本7也添加create_time字段
                if "create_time" not in columns:
                    con.execute("ALTER TABLE notes ADD COLUMN create_time INT DEFAULT 0;")
                    
                    # 对于已有数据，我们需要从原始笔记数据中提取create_time
                    # 但由于性能考虑，我们只在访问时动态提取，不在这里批量更新
                    logger.info("Added create_time column. Existing notes will have create_time=0.")

                # 版本7添加tag和ext字段
                if "tag" not in columns:
                    con.execute("ALTER TABLE notes ADD COLUMN tag TEXT;")
                    logger.info("Added tag column.")
                
                if "ext" not in columns:
                    con.execute("ALTER TABLE notes ADD COLUMN ext TEXT;")
                    logger.info("Added ext column for storing parsed content fields.")

        self.config.set_config_value("DB_VERSION", str(CURRENT_DB_VERSION))

        if need_resync:
            self.config.set_config_value("USN", "0")
            raise DatabaseResyncRequiredError


class NoteBookStorage(SqliteStorage):  # noqa: WPS214
    def add_notebooks(self, notebooks: Iterable[Notebook]) -> None:
        if logger.getEffectiveLevel() == logging.DEBUG:  # pragma: no cover
            for nb in notebooks:
                nb_info = log_format_notebook(nb)
                logger.debug(f"Adding/updating notebook {nb_info}")

        with self.db as con:
            con.executemany(
                "replace into notebooks(guid, name, stack) values (?, ?, ?)",
                ((nb.guid, nb.name, nb.stack) for nb in notebooks),  # noqa: WPS441
            )

    def iter_notebooks(self) -> Iterator[Notebook]:
        with self.db as con:
            cur = con.execute(
                "select guid, name, stack from notebooks",
            )

            yield from (
                Notebook(
                    guid=row["guid"],
                    name=row["name"],
                    stack=row["stack"],
                )
                for row in cur
            )

    def get_notebook_notes_count(self, notebook_guid: str) -> int:
        with self.db as con:
            cur = con.execute(
                "select COUNT(guid) from notes"
                " where notebook_guid=? and is_active=1 and raw_note is not NULL",
                (notebook_guid,),
            )

            return int(cur.fetchone()[0])

    def expunge_notebooks(self, guids: Iterable[str]) -> None:
        with self.db as con:
            con.executemany("delete from notebooks where guid=?", ((g,) for g in guids))

    def add_linked_notebook(
        self, l_notebook: LinkedNotebook, notebook: Notebook
    ) -> None:
        if logger.getEffectiveLevel() == logging.DEBUG:  # pragma: no cover
            logger.debug(
                f"Adding/updating linked notebook '{l_notebook.shareName}'"
                f" [{l_notebook.guid}] -> [{notebook.guid}]"
            )

        with self.db as con:
            con.execute(
                "replace into notebooks_linked(guid, notebook_guid) values (?, ?)",
                (l_notebook.guid, notebook.guid),
            )

    def get_notebook_by_linked_guid(self, l_notebook_guid: str) -> Notebook:
        with self.db as con:
            cur = con.execute(
                "select notebooks.guid, notebooks.name, notebooks.stack"
                " from notebooks_linked"
                " join notebooks"
                " on notebooks.guid=notebooks_linked.notebook_guid"
                " where notebooks_linked.guid=?",
                (l_notebook_guid,),
            )

            row = cur.fetchone()

            if row is None:
                raise ValueError(
                    f"No local notebooks found for linked notebook {l_notebook_guid}"
                )

            return Notebook(
                guid=row["guid"],
                name=row["name"],
                stack=row["stack"],
            )

    def get_linked_notebook_usn(self, l_notebook_guid: str) -> int:
        with self.db as con:
            cur = con.execute(
                "select usn from notebooks_linked where guid=?",
                (l_notebook_guid,),
            )

            res = cur.fetchone()

            if res is None:
                return 0

            return int(res[0])

    def set_linked_notebook_usn(self, l_notebook_guid: str, usn: int) -> None:
        with self.db as con:
            con.execute(
                "update notebooks_linked set usn=? where guid=?",
                (usn, l_notebook_guid),
            )

    def expunge_linked_notebooks(self, guids: Iterable[str]) -> None:
        with self.db as con:
            con.executemany(
                "delete from notebooks_linked where guid=?", ((g,) for g in guids)
            )


class NoteStorage(SqliteStorage):  # noqa: WPS214
    def add_notes_for_sync(self, notes: Iterable[Note]) -> None:
        if logger.getEffectiveLevel() == logging.DEBUG:  # pragma: no cover
            for note in notes:
                n_info = log_format_note(note)
                logger.debug(f"Scheduling note for sync {n_info}")

        with self.db as con:
            con.executemany(
                "replace into notes(guid, title, notebook_guid, create_time, update_time, sync_time, tag, ext) values (?, ?, ?, ?, ?, ?, ?, ?)",
                ((
                    n.guid,
                    n.title,
                    n.notebookGuid,
                    n.created,
                    n.updated,
                    int(time.time() * 1000),
                    json.dumps(n.tagNames, ensure_ascii=False) if n.tagNames else None,
                    json.dumps(parse_note_content(n.content), ensure_ascii=False) if n.content else None
                ) for n in notes),
            )

    def add_note(self, note: Note) -> None:
        if logger.getEffectiveLevel() == logging.DEBUG:  # pragma: no cover
            n_info = log_format_note(note)
            logger.debug(f"Adding/updating note {n_info}")

        note_deflated = lzma.compress(pickle.dumps(note))

        # 解析笔记内容中的ext字段
        ext_data = parse_note_content(note.content) if note.content else {}
        
        with self.db as con:
            con.execute(
                "replace into notes(guid, title, notebook_guid, is_active, raw_note, create_time, update_time, sync_time, tag, ext)"
                " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    note.guid,
                    note.title,
                    note.notebookGuid,
                    note.active,
                    note_deflated,
                    note.created,
                    note.updated,
                    int(time.time() * 1000),
                    json.dumps(note.tagNames, ensure_ascii=False) if note.tagNames else None,
                    json.dumps(ext_data, ensure_ascii=False) if ext_data else None,
                ),
            )

        logger.debug(f"Added note [{note.guid}]")

    def iter_notes(
        self,
        notebook_guid: str,
        after_create: Optional[int] = None,
        after_update: Optional[int] = None,
        after_sync: Optional[int] = None,
    ) -> Iterator[Note]:
        for note_guid in self._get_notes_by_notebook(
            notebook_guid, after_create=after_create, after_update=after_update, after_sync=after_sync
        ):
            with self.db as con:
                cur = con.execute(
                    "select title, guid, raw_note, create_time, update_time, sync_time"
                    " from notes"
                    " where guid=? and raw_note is not NULL",
                    (note_guid,),
                )

                row = cur.fetchone()

                raw_note = self._get_raw_note(
                    row["title"],
                    row["guid"],
                    row["raw_note"],
                    row["create_time"],
                    row["update_time"],
                    row["sync_time"],
                )

                if raw_note:
                    yield raw_note

    def iter_notes_trash(
        self, after_create: Optional[int] = None, after_update: Optional[int] = None, after_sync: Optional[int] = None
    ) -> Iterator[Note]:
        query = (
            "select title, guid, raw_note, create_time, update_time, sync_time"
            " from notes"
            " where is_active=0 and raw_note is not NULL"
        )
        params: list[Union[str, int]] = []

        if after_create:
            query += " and create_time > ?"
            params.append(after_create)

        if after_update:
            query += " and update_time > ?"
            params.append(after_update)

        if after_sync:
            query += " and sync_time > ?"
            params.append(after_sync)

        query += " order by title COLLATE NOCASE"

        with self.db as con:
            cur = con.execute(query, params)

            for row in cur:
                raw_note = self._get_raw_note(
                    row["title"],
                    row["guid"],
                    row["raw_note"],
                    row["create_time"],
                    row["update_time"],
                    row["sync_time"],
                )

                if raw_note:
                    yield raw_note

    def check_notes(self, mark_corrupt: bool) -> Iterator[Optional[Note]]:
        with self.db as con:
            cur = con.execute(
                "select title, guid, raw_note, update_time, sync_time"
                " from notes"
                " where raw_note is not NULL",
            )

            for row in cur:
                raw_note = self._get_raw_note(
                    row["title"],
                    row["guid"],
                    row["raw_note"],
                    row["create_time"],
                    row["update_time"],
                    row["sync_time"],
                )

                if raw_note:
                    yield raw_note
                elif mark_corrupt:
                    logger.info(
                        f"Marking '{row['title']}' [{row['guid']}] note for re-download"
                    )
                    self._mark_note_for_redownload(row["guid"])
                    yield None

    def get_notes_for_sync(self) -> tuple[NoteForSync, ...]:
        with self.db as con:
            cur = con.execute(
                "select notes.guid, title, notebooks_linked.guid as l_notebook"
                " from notes"
                " left join notebooks_linked"
                " using (notebook_guid)"
                " where raw_note is NULL"
            )

            notes = (
                NoteForSync(
                    guid=row["guid"],
                    title=row["title"],
                    linked_notebook_guid=row["l_notebook"],
                )
                for row in cur.fetchall()
            )

            return tuple(notes)

    def expunge_notes(self, guids: Iterable[str]) -> None:
        with self.db as con:
            con.executemany("delete from notes where guid=?", ((g,) for g in guids))

    def expunge_notes_by_notebook(self, notebook_guid: str) -> None:
        with self.db as con:
            con.execute("delete from notes where notebook_guid=?", (notebook_guid,))

    def get_notes_count(self, is_active: bool = True) -> int:
        with self.db as con:
            cur = con.execute(
                "select COUNT(guid)"
                " from notes"
                " where is_active=? and raw_note is not NULL",
                (is_active,),
            )

            return int(cur.fetchone()[0])

    def _get_notes_by_notebook(
        self,
        notebook_guid: str,
        after_create: Optional[int] = None,
        after_update: Optional[int] = None,
        after_sync: Optional[int] = None,
    ) -> list[str]:
        """Due to wrong idx_notes index, SQLite creates a temporary table on
            from notes where notebook_guid=? and is_active=1
            order by title COLLATE NOCASE
        which may cause a memory leak. This method sorts notes alphabetically
        to prevent SQLite from creating a sort table."""

        query = (
            "select guid, title"
            " from notes"
            " where notebook_guid=? and is_active=1 and raw_note is not NULL"
        )
        params: list[Union[str, int]] = [notebook_guid]

        if after_create:
            query += " and create_time > ?"
            params.append(after_create)

        if after_update:
            query += " and update_time > ?"
            params.append(after_update)

        if after_sync:
            query += " and sync_time > ?"
            params.append(after_sync)

        with self.db as con:
            cur = con.execute(query, params)

            sorted_notes = sorted(cur, key=lambda x: x["title"])

            return [r["guid"] for r in sorted_notes]

    def _get_raw_note(
        self,
        note_title: str,
        note_guid: str,
        raw_note: bytes,
        create_time: int,
        update_time: int,
        sync_time: int,
    ) -> Optional[Note]:
        try:
            return pickle.loads(lzma.decompress(raw_note))
        except Exception:
            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.exception(f"Note '{note_title}' [{note_guid}] is corrupt")

            logger.warning(f"Note '{note_title}' [{note_guid}] is corrupt")

        return None

    def _mark_note_for_redownload(self, note_guid: str) -> None:
        with self.db as con:
            con.execute(
                "update notes set raw_note=NULL, is_active=NULL, update_time=?, sync_time=? where guid=?",
                (int(time.time()), int(time.time()), note_guid),
            )

    @log_operation_time
    def update_ext_from_raw_notes(self, batch_size: int = 100) -> tuple[int, int]:
        """从raw_note字段解析内容并更新ext字段
        
        分批遍历所有有raw_note的笔记，解析内容中的字段，更新ext字段。
        分批处理可以避免内存溢出，适用于大数据量场景。
        
        Args:
            batch_size: 每批处理的笔记数量，默认100条
        
        Returns:
            (更新的笔记数量, 失败的笔记数量)
        """
        updated_count = 0
        failed_count = 0
        processed_count = 0
        
        logger.info(f"Starting to update ext from raw_notes with batch_size={batch_size}")
        
        with self.db as con:
            # 首先获取总数量
            cur = con.execute(
                "SELECT COUNT(*) FROM notes WHERE raw_note IS NOT NULL"
            )
            total_count = cur.fetchone()[0]
            logger.info(f"Total notes to process: {total_count}")
            
            if total_count == 0:
                return 0, 0
            
            # 分批处理
            offset = 0
            while offset < total_count:
                # 获取当前批次的数据
                cur = con.execute(
                    "SELECT guid, title, raw_note FROM notes WHERE raw_note IS NOT NULL LIMIT ? OFFSET ?",
                    (batch_size, offset)
                )
                
                rows = cur.fetchall()
                if not rows:
                    break
                
                batch_updated = 0
                batch_failed = 0
                
                for row in rows:
                    try:
                        # 解析raw_note
                        note = pickle.loads(lzma.decompress(row["raw_note"]))
                        
                        if not note or not note.content:
                            processed_count += 1
                            continue
                        
                        # 解析内容
                        ext_data = parse_note_content(note.content)
                        
                        if ext_data:
                            # 更新ext字段
                            con.execute(
                                "UPDATE notes SET ext = ? WHERE guid = ?",
                                (json.dumps(ext_data, ensure_ascii=False), row["guid"])
                            )
                            updated_count += 1
                            batch_updated += 1
                            logger.debug(f"Updated ext for note '{row['title']}' [{row['guid']}]: {ext_data}")
                        
                        processed_count += 1
                        
                    except Exception as e:
                        failed_count += 1
                        batch_failed += 1
                        processed_count += 1
                        logger.warning(f"Failed to process note '{row['title']}' [{row['guid']}]: {e}")
                        continue
                
                # 提交当前批次
                con.commit()
                
                logger.info(
                    f"Batch {offset // batch_size + 1}: "
                    f"processed {len(rows)}, updated {batch_updated}, failed {batch_failed} "
                    f"(total: {processed_count}/{total_count})"
                )
                
                offset += batch_size
        
        logger.info(f"Completed: total processed {processed_count}, updated {updated_count}, failed {failed_count}")
        return updated_count, failed_count


class TasksStorage(SqliteStorage):  # noqa: WPS214
    def add_tasks(self, tasks: Iterable[Task]) -> None:
        for task in tasks:
            self.add_task(task)

    def add_task(self, task: Task) -> None:
        logger.debug(f"Adding/updating task [{task.taskId}] note_id [{task.parentId}]")

        task_deflated = lzma.compress(task.to_json().encode("utf-8"))

        with self.db as con:
            con.execute(
                "replace into tasks(guid, note_guid, raw_task) values (?, ?, ?)",
                (task.taskId, task.parentId, task_deflated),
            )

    def iter_tasks(self, note_guid: str) -> Iterator[Task]:
        with self.db as con:
            cur = con.execute(
                "select guid, raw_task from tasks where note_guid=?",
                (note_guid,),
            )

            for row in cur:
                raw_task = self._get_raw_task(row["guid"], row["raw_task"])

                if raw_task:
                    yield raw_task

    def expunge_tasks(self, guids: Iterable[str]) -> None:
        with self.db as con:
            con.executemany("delete from tasks where guid=?", ((g,) for g in guids))

    def _get_raw_task(self, task_guid: str, raw_task: bytes) -> Optional[Task]:
        try:
            return Task.from_json(lzma.decompress(raw_task).decode("utf-8"))
        except Exception:
            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.exception(f"Task [{task_guid}] is corrupt")

            logger.warning(f"Task [{task_guid}] is corrupt")

        return None


class RemindersStorage(SqliteStorage):  # noqa: WPS214
    def add_reminders(self, reminders: Iterable[Reminder]) -> None:
        for reminder in reminders:
            self.add_reminder(reminder)

    def add_reminder(self, reminder: Reminder) -> None:
        logger.debug(
            f"Adding/updating reminder [{reminder.reminderId}] task_id [{reminder.sourceId}]"
        )

        reminder_deflated = lzma.compress(reminder.to_json().encode("utf-8"))

        with self.db as con:
            con.execute(
                "replace into reminders(guid, task_guid, raw_reminder)"
                " values (?, ?, ?)",
                (reminder.reminderId, reminder.sourceId, reminder_deflated),
            )

    def iter_reminders(self, task_guid: str) -> Iterator[Reminder]:
        with self.db as con:
            cur = con.execute(
                "select guid, raw_reminder from reminders where task_guid=?",
                (task_guid,),
            )

            for row in cur:
                raw_reminder = self._get_raw_reminder(row["guid"], row["raw_reminder"])

                if raw_reminder:
                    yield raw_reminder

    def expunge_reminders(self, guids: Iterable[str]) -> None:
        with self.db as con:
            con.executemany("delete from reminders where guid=?", ((g,) for g in guids))

    def _get_raw_reminder(self, guid: str, raw_reminder: bytes) -> Optional[Reminder]:
        try:
            return Reminder.from_json(lzma.decompress(raw_reminder).decode("utf-8"))
        except Exception:
            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.exception(f"Reminder [{guid}] is corrupt")

            logger.warning(f"Reminder [{guid}] is corrupt")

        return None


class ConfigStorage(SqliteStorage):
    def set_config_value(self, name: str, config_value: str) -> None:
        with self.db as con:
            con.execute(
                "replace into config(name, value) values (?, ?)",
                (name, config_value),
            )

    def get_config_value(self, name: str) -> str:
        with self.db as con:
            cur = con.execute("select value from config where name=?", (name,))
            res = cur.fetchone()

            if not res:
                raise KeyError(f"Config ID {name} not found in database!")

            return str(res[0])
