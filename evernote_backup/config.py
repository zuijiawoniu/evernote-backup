API_DATA_YINXIANG = b"WFgyaS4uNmJ4bWN+OHp2ZTEpbGtvNDg6MW0wPmM9ZmFn"
API_DATA_EVERNOTE = b"eW91c3V3dn9mYjF2az48bzM7Pm4wZzdlZzpk"

CURRENT_DB_VERSION = 7

# 笔记内容中关键词到ext字段key的映射关系
# 用于从笔记内容中解析summary/confidence/retitle/keywords/reflection/author字段
NOTE_CONTENT_KEYWORDS_MAP = {
    "摘要": "summary",
    "置信度": "confidence",
    "重写文章标题": "retitle",
    "关键词": "keywords",
    "反思": "reflection",
    "作者识别": "auto_author",
}