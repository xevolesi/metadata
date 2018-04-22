# Коллективная разработка программного обеспечения
## Структура проекта

    |
    |
    | - data_base_sources
    |           |
    |           |
    |           | - DBClasses.py
    |           |
    |           | - DBDConst.py
    |
    |
    | - source_xmls
    |           |
    |           |
    |           | - PRJADM.xml
    |           |
    |           | - TASKS.xml
    |
    |
    | -       utils
    |           |
    |           |
    |           | - helpers.py
    |           |
    |           | - minidom_fixed.py
    |           |
    |           | - RAM_to_XML_translation.py
    |           |
    |           | - XML_to_RAM_translation.py
    |           |
    |           | - XMLComparator.py
    |
    |
    | - main.py

## Легенда:

### Директория *data_base_sources/*

Содержит в себе 2 модуля.

Описание модулей:
 - Модуль *DBClasses* содержит в себе описание классов для представления объектов базы данных в RAM.

    Содержит классы:
     - Schema;
     - Table;
     - Field;
     - Domain;
     - Constraint;
     - Index.

 - Модуль *DBD_const* содержит в себе DDL базы данных.

### Директория *source_xmls/*
Содержит в себе исходные xml файлы: *PRJADM.xml* и *TASKS.xml*. Сюда же сохраняется xml файл, генерируемый методом RAM → XML.

### Директория *utils/*
Содержит в себе вспомогательные модули (*minidom_fixed*, *helpers*), модули трансляции (*RAM_to_XML_translation*, *XML_to_RAM_translation*) и класс *XMLComparator*.

Описание содержимого:
 - Модуль *minidom_fixed* - попытка исправления недостатка xml.dom.minidom, состоящего в том, что xml.dom.mindom.Document.writexml не сохраняет последовательность атрибутов тэга;
 - Модуль *helpers* - набор функций для отладки;
 - Модуль *XML_to_RAM_translation* содержит в себе класс *XMLToRAMTranslator* и пользовательские исключения (*DBException*, *WrongAttributeException*, *WrongNodeException*, *WrongPropertyException*).

   Описание содержимого:
    - *XMLToRAMTranslator* - класс, производящий трансляцию исходных XML файлов в их RAM представление (решение задачи [1]);
    - Класс *DBException* является наследником встроенного класса *Exception* и родителем классов *WrongAttributeExcepton*, *WrongNodeException* и *WrongPropertyException*. В его конструкторе добавлены два атрибута: *expected*, *got*. Первый атрибут отвечает за ожидаемые значения, а второй за получаемые;
    - Класс *WrongAttributeException* является потомком класса *DBException*. Определяет исключение, выбрасываемое при получении неожиданного атрибута при парсинге xml файла;
    - Класс *WrongNodeException* является потомком класса *DBException*. Определяет исключение, выбрасываемое при получении неожиданного элемента при парсинге xml файла;
    - Класс *WrongPropertyException* является потомком класса *DBException*. Определяет исключение, выбрасываемое при получении неожиданного значения атрибута *props* при парсинге xml файла.

 - Модуль *RAM_to_XML_translation* содержит в себе класс *RAMToXMLTranslator* и пользовательские исключения (*EmptyFieldError*, *SchemaNotFoundError*).

   Описание содержимого:
    - *RAMToXMLTranslator* - класс, производящий трансляцию полученного RAM представление в XML представление (решение задачи [2]);
    - Класс *SchemaNotFoundError* является наследником класса *ValueError*. Определяет ошибку, генерируемую в случае, когда не обнаружена схема базы данных во время трансляции из RAM в XML;
    - Класс *EmptyFieldError* является наследником класса *ValueError*. Определяет ошибку, генерируемую в случае, когда обнаружено пустое поле у таблицы во время трансляции из RAM в XML.

- Класс *XMLComparator*. Наследник класса *unittest.TestCase*. Предназначен для сравнения двух xml файлов, для чего используется метод *XMLComparator.compare()*.
