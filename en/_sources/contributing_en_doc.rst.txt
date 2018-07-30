Bigflow Contributing English Document
=======================================

目前Bigflow 的英文文档需要同步更新。Bigflow 的文档是通过Sphinx 生成的，Bigflow 英文文档使用的是
Sphinx 的国际化支持功能。

文档更新
------------

1. git clone -b doc https://github.com/baidu/bigflow.git

2. cd bigflow/doc/locales/en/LC_MESSAGES

3. 找到对应.po 文件进行更新：

   a. msgid 为要完成翻译的目标语句

   b. msgstr 目标语句的翻译

.. image:: static/po_file_format.png
       :align: center
       :height: 80px
       :width: 200px

已经被翻译过的msgid 被修改后，需要删除fuzzy 行

.. image:: static/po_file_format_update.png
       :align: center
       :height: 80px
       :width: 200px
