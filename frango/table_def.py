from __future__ import annotations

from dataclasses import dataclass, field

from dataclass_wizard import LoadMixin
from typing import Dict, Sequence

import sqlglot.expressions as exp

from frango.sql_adaptor import SQLDef, PARAMS_ARG_KEY, sql_parse_one


# noinspection SqlNoDataSourceInspection,SqlResolve
@dataclass
class Article(LoadMixin, SQLDef):  # type: ignore[misc]
    id: str
    timestamp: int
    aid: int
    title: str
    category: str
    abstract: str
    articleTags: str
    authors: str
    language: str
    image: str
    video: str

    @staticmethod
    def __primary_key__() -> str:
        return "aid"

    @classmethod
    def sql_hook_create(cls) -> exp.Expression:
        stmt = sql_parse_one('''
            CREATE INDEX idx_Article ON Article(aid);
            ''')
        assert isinstance(stmt, exp.Create)
        return stmt

    def sql_hook_insert(self) -> exp.Expression:
        insert = sql_parse_one('''
INSERT INTO BeRead (
    id, timestamp, aid, readNum, readUidList, commentNum, commentUidList, agreeNum, agreeUidList, shareNum, shareUidList
) VALUES ( 'read-' || :aid, :timestamp, :aid, 0, '', 0, '', 0, '', 0, '' )
        ''')
        assert isinstance(insert, exp.Insert)
        insert.set(PARAMS_ARG_KEY, self.to_dict())
        return insert


@dataclass
class User(SQLDef):
    timestamp: int
    id: str
    uid: int
    name: str
    gender: str
    email: str
    phone: str
    dept: str
    grade: str
    language: str
    region: str
    role: str
    preferTags: str
    obtainedCredits: int

    @classmethod
    def sql_hook_create(cls) -> exp.Expression:
        stmt = sql_parse_one('''
            CREATE INDEX idx_User ON User(uid);
        ''')
        assert isinstance(stmt, exp.Create)
        return stmt

    @staticmethod
    def __primary_key__() -> str:
        return "uid"


# noinspection SqlNoDataSourceInspection,SqlResolve
@dataclass
class Read(SQLDef):
    timestamp: int
    id: str
    uid: int
    aid: int
    readTimeLength: int
    agreeOrNot: bool
    commentOrNot: bool
    shareOrNot: bool
    commentDetail: str

    @classmethod
    def sql_hook_create(cls) -> exp.Expression:
        stmt = sql_parse_one('''
            CREATE INDEX idx_Read ON Read(aid, timestamp);
        ''')
        assert isinstance(stmt, exp.Create)
        return stmt

    def sql_hook_insert(self) -> exp.Expression:
        update = sql_parse_one('''
UPDATE BeRead
SET 
    readNum = readNum + 1,
    readUidList = readUidList || iif(readUidList = '', '', ',') || :uid,
    commentNum = commentNum + iif(:commentOrNot, 1, 0),
    commentUidList = iif(:commentOrNot, commentUidList || iif(commentUidList = '', '', ',') || :uid, commentUidList),
    agreeNum = agreeNum + iif(:agreeOrNot, 1, 0),
    agreeUidList = iif(:agreeOrNot, agreeUidList || iif(agreeUidList = '', '', ',') || :uid, agreeUidList),
    shareNum = shareNum + iif(:shareOrNot, 1, 0),
    shareUidList = iif(:shareOrNot, shareUidList || iif(shareUidList = '', '', ',') || :uid, shareUidList)
WHERE aid = :aid
                ''')
        assert isinstance(update, exp.Update)
        update.set(PARAMS_ARG_KEY, self.to_dict())
        return update

    @staticmethod
    def sql_hook_bulk_load(reads: Sequence[Read]) -> exp.Insert:
        def append_id(origin_list: str, id_: int) -> str:
            if len(origin_list) == 0:
                return str(id_)
            else:
                return origin_list + ',' + str(id_)

        be_read_dict: Dict[int, BeRead] = dict()
        for read in reads:
            be_read = be_read_dict.get(read.aid, BeRead(id=f'read-{read.aid}', timestamp=read.timestamp, aid=read.aid))

            be_read.readNum += 1
            append_id(be_read.readUidList, read.uid)
            if read.agreeOrNot:
                be_read.agreeNum += 1
                append_id(be_read.agreeUidList, read.uid)
            if read.shareOrNot:
                be_read.shareNum += 1
                append_id(be_read.shareUidList, read.uid)
            if read.commentOrNot:
                be_read.commentNum += 1
                append_id(be_read.commentUidList, read.uid)

            be_read_dict[read.aid] = be_read

        insert = sql_parse_one('''
INSERT INTO BeRead (
    id, timestamp, aid, readNum, readUidList, commentNum, commentUidList, agreeNum, agreeUidList, shareNum, shareUidList
) VALUES (
    :id, :timestamp, :aid, :readNum, :readUidList, :commentNum, :commentUidList, :agreeNum, :agreeUidList,
    :shareNum, :shareUidList
)
        ''')
        assert isinstance(insert, exp.Insert)
        insert.set(PARAMS_ARG_KEY, [item.to_dict() for item in be_read_dict.values()])
        return insert


@dataclass
class BeRead(SQLDef):
    id: str
    timestamp: int
    aid: int
    readNum: int = field(default=0)
    readUidList: str = field(default="")
    commentNum: int = field(default=0)
    commentUidList: str = field(default="")
    agreeNum: int = field(default=0)
    agreeUidList: str = field(default="")
    shareNum: int = field(default=0)
    shareUidList: str = field(default="")


# noinspection SqlNoDataSourceInspection,SqlResolve
@dataclass
class PopularRank(SQLDef):
    id: str
    timestamp: str
    temporalGranularity: str  # daily | weekly | monthly
    articleAidList: str

    @classmethod
    def sql_create(cls) -> exp.Create:
        create = sql_parse_one('''
-- Create the PopularRank view
CREATE VIEW PopularRank AS
WITH RECURSIVE
-- Generate a series of dates
DateSeries AS (
    SELECT datetime('2017-01-01') AS date
    UNION ALL
    SELECT date(date, '+1 day')
    FROM DateSeries
    WHERE date < datetime('2018-12-31')
),
-- Daily aggregation
DailyAgg AS (
    SELECT
        strftime('%Y-%m-%d', timestamp / 1000) AS day_start,
        aid,
        SUM(readNum) AS total_reads
    FROM BeRead
    GROUP BY strftime('%Y-%m-%d', timestamp / 1000), aid
),
-- Weekly aggregation
WeeklyAgg AS (
    SELECT
        strftime('%Y-%W', timestamp / 1000) AS week_start,
        aid,
        SUM(readNum) AS total_reads
    FROM BeRead
    GROUP BY strftime('%Y-%W', timestamp / 1000), aid
),
-- Monthly aggregation
MonthlyAgg AS (
    SELECT
        strftime('%Y-%m', timestamp / 1000) AS month_start,
        aid,
        SUM(readNum) AS total_reads
    FROM BeRead
    GROUP BY strftime('%Y-%m', timestamp / 1000), aid
),
-- Rank articles for each temporal granularity
DailyRank AS (
    SELECT
        day_start AS timestamp,
        'daily' AS temporalGranularity,
        group_concat(aid, ',') AS articleAidList
    FROM (
        SELECT
            day_start,
            aid,
            total_reads,
            ROW_NUMBER() OVER (PARTITION BY day_start ORDER BY total_reads DESC) AS rank
        FROM DailyAgg
    )
    WHERE rank <= 5
    GROUP BY day_start
),
WeeklyRank AS (
    SELECT
        week_start AS timestamp,
        'weekly' AS temporalGranularity,
        group_concat(aid, ',') AS articleAidList
    FROM (
        SELECT
            week_start,
            aid,
            total_reads,
            ROW_NUMBER() OVER (PARTITION BY week_start ORDER BY total_reads DESC) AS rank
        FROM WeeklyAgg
    )
    WHERE rank <= 5
    GROUP BY week_start
),
MonthlyRank AS (
    SELECT
        month_start AS timestamp,
        'monthly' AS temporalGranularity,
        group_concat(aid, ',') AS articleAidList
    FROM (
        SELECT
            month_start,
            aid,
            total_reads,
            ROW_NUMBER() OVER (PARTITION BY month_start ORDER BY total_reads DESC) AS rank
        FROM MonthlyAgg
    )
    WHERE rank <= 5
    GROUP BY month_start
)
-- Combine all rankings into the final view
SELECT * FROM DailyRank
UNION ALL
SELECT * FROM WeeklyRank
UNION ALL
SELECT * FROM MonthlyRank;
        ''')
        assert isinstance(create, exp.Create)
        return create
