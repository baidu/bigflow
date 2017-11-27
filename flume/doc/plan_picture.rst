=========================
Flume中Plan可视化图的生成
=========================
Flume 中 `Plan <http://project.baidu.com:8999/flume/doc/planner.html>`_
可视化图可以很清晰地展示Plan计划流程,
主要用于辅助对Plan的理解, 降低使用Pass对Plan进行优化时的调试难度。

本教程的主要内容是如何生成Flume中Plan的可视化图, 教程包含了主要的准备工作以及生成图的主要步骤,
并且给出了重要的说明以及关键的代码。

Pass以及Plan相关的概念, 请参考 `Plan <http://project.baidu.com:8999/flume/doc/planner.html>`_ 。

第一部分 总览
-------------
Flume源代码中, 在对Plan进行测试的时候会生成与Plan相关的.dot文件,
这些.dot文件是对每一个阶段的Plan图的描述, Plan可视化的过程就是将这些.dot文件通过转化,
最终生成可视化的拓扑结构图, 从而直观地展示Plan经过优化器前后的拓扑图变化。

.dot文件需要通过调用Pass对Plan进行优化生成, 转化成可视化图的过程需要调用Graphviz工具的支持,
Graphviz可以将.dot文件的内容处理成为图片的格式, 从而达到可视化的目的。
教程将会在第二部分描述需要的准备工作, 第三部分描述如何将*.dot文件转化成为图片的格式,
第四部分对可视化图进行简单的说明。

第二部分 准备工作
-----------------

本章节内容主要是针对百度的内部环境, 给出的准备工作说明具有一定的特殊性。

**百度开发需要使用测试机器, 而测试机器并不具有图形界面,
所以无法直接在开发机器上浏览生成的可视化图片,
因此需要另外具有linux桌面的机器或者是linux虚拟机, 当然也可以使用windows机器。**

在准备工作中, 将会分别介绍linux机器的以及windows机器的配置与工具安装。
由于.dot文件都是在开发机器上, 所以每次生成.dot对应图片的时候,
都需要远程从开发机上拷贝.dot文件, 为了方便拷贝, 可以使用开发机自带的samba服务。

2.1 开发机samba服务配置
~~~~~~~~~~~~~~~~~~~~~~~~

**samba密码修改**

* 1. 登陆开发机申请界面  http://ocean.baidu.com/

* 2. 找到开发机, 选择操作->更多操作->修改samba密码, 完成修改samba密码

    .. image:: static/plan_ocean_samba.png
       :width: 800px

2.2 linux机器配置与graphviz安装
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

2.2.1 说明
##########

常用的linux系统包括ubuntu、fedora、centos以及redhat等版本,
这里不具体介绍linux系统的安装流程, 需要的同学可以自行百度之。
安装linux系统可以选择在公司发放的机器上安装双系统,
也可以选择安装linux虚拟机, 教程中使用的是linux虚拟机, 系统为fedora 20桌面版。

若是要安装虚拟机, 可以使用常用虚拟机软件VMware, 下载地址有很多,
百度可以直接搜索到最新的版本以及相应的注册码, 当然如果觉得有必要的话,
也可以选择购买VMware软件。安装好虚拟机之后, 可以下载相应linux系统的镜像文件,
然后按照网上常用教程安装即可, 整个过程没有特别复杂的地方。

若是采用双系统的方式, 安装方法请自行百度之。

2.2.2 graphviz安装
###################
安装graphviz的方法主要分为两类, 一类是采用源码编译,
另一类就是使用linux自带的apt-get或者yum等命令进行安装。
（不在开发机上安装graphviz的原因, 除了开发机无法浏览突破外,
还有一个比较重要的原因是由于开发机上没有sudo权限, 在编译或者安装过程中若出现错误,
比较难以处理）

出于便利性的考虑, 个人比较建议使用linux自带安装命令来完成安装,
有兴趣的同学可以使用源码编译。

Ubuntu

    .. code-block:: bash

        sudo apt-get install graphviz

Fedora

    .. code-block:: bash

        sudo yum install graphviz

dot命令是最主要的命令, 安装完成之后, 测试dot命令是否已经安装成功。

    .. code-block:: bash

        dot -h

若是返回以下结果, 说明graphviz已经安装成功。

    .. image:: static/plan_dot_cmd.png
       :width: 400px

2.2.3 linux连接samba服务
########################

不同的linux机器, 连接samba的方式都不太一样, 教程仍以fedora系统进行说明, 官网的说明如下：

http://docs.fedoraproject.org/en-US/Fedora/12/html/Deployment_Guide/s1-samba-connect-share.html

* 1. 打开fedora系统的文件窗口
* 2. 点击File->EnterLocation...
* 3. 输入smb://<servername>/<sharename>, 其中servername是开发机地址,
sharename是你登陆开发机的用户名, 回车之后会让输入密码,
密码就是你自己之前设置的samba密码

    .. image:: static/plan_linux_samba.png
       :width: 800px

2.3 windows机器配置与graphviz的安装
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

2.3.1 graphviz安装
###################

* 1. 下载graphviz的windows安装版本:

    http://www.graphviz.org/Download_windows.php

* 2. 下载完成后双击安装, 跟着流程完成安装

2.3.2 windows连接samba服务
##########################

关于windows连接samba服务的流程, 可以参考以下教程：

    http://linux.chinaunix.net/techdoc/beginner/2009/07/09/1122864.shtml

第三部分 Plan可视化图生成
--------------------------

.dot文件的生成需要在开发机上手动运行单测, 生成.dot文件之后,
再将所有文件拷贝到虚拟机或者windows机器上, 之后进行图片的生成。

3.1 .dot文件生成
~~~~~~~~~~~~~~~~~

关于如何git clone代码, 请参考：

    http://project.baidu.com:8999/flume/doc/dev-tutorial.html#clone

以下内容操作在开发机上执行, 并且默认读者了解blade测试框架, 了解clone下来的代码。

* 1. 命令行进入..../baidu/flume/planner/dce/
* 2. 执行以下命令

    .. code-block:: bash

        blade build ...

* 3. 命令行进入..../build64_release/flume/planner/dce/
* 4. 执行以下命令

    .. code-block:: bash

        ./dce_planner_test

* 5. 在当前文件夹下使用ls命令可以查看到生成的以大写字母开头的文件夹, 确认生成的文件

每个大写文件夹代表了一个Pass的完整流程, 文件夹下所有的.dot文件代表了每一次优化操作之后Plan图的结构。

3.2 linux下graphviz的使用
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

我们将在自己的linux机器上使用graphviz处理.dot文件, 需要将开发机上的.dot文件拷贝至本地,
从而可以使用graphviz的相关命令。

linux下, 我们使用graphviz中的dot命令将.dot文件处理成svg格式图片,
.dot的具体用法可以在命令行下查询, 基本的用法如下：

   .. code-block:: bash

        dot input.dot -Tsvg -o output.svg
        dot input.dot -Tpng -o output.png
        dot input.dot -Tbmp -o output.bmp

其中input.dot代表了输入的.dot文件的路径, output.*代表了输出文件的路径,
-T*代表了文件的类型, dot支持的文件类型比较多, 这里只是罗列出了svg, png, bmp三种文件类型。

另外, 也可以编写批处理文件来同时处理多个.dot文件, 样例文件可以在以下路径中找到：

..../baidu/flume/runtime/dce/static/draw.sh

以下是修改过的一个版本：

    .. code-block:: bash

        #!/bin/bash
        OUT=~/output
        mkdir -p $OUT
        rm $OUT/*
        for file in *.dot; do
            dot -Tsvg -o $OUT/${file%.dot}.svg $file 2> /dev/null
        done

3.3 windows下graphviz的使用
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

在windows下使用graphviz的前提也是将开发机上的.dot拷贝至本地。windows下的使用方式有两种,
第一种为界面的方式, 另外一种为命令行的方式。

这两种方式所需要的执行文件都在graphviz的安装路径下,
教程的安装路径为d:\\Program Files (x86)\\Graphviz2.38,
其中dot.exe与gvedit.exe均在安装路径的bin文件夹下。

3.3.1 windows下界面操作
#########################

* 1. 打开d:\\Program Files (x86)\\Graphviz2.38\\bin\\gvedit.exe
* 2. 点击open图标, 打开相应的.dot文件, 同时生成图片

    .. image:: static/plan_gvedit.png
       :width: 800px

3.3.2 windows下命令行操作
#########################

windows下命令行的方式与linux下格式一样, 基本的格式可以参考以下命令：

    .. code-block:: bash

        dot.exe d:\input.dot -Tpng -o d:\output.png

dot.exe在d:\Program Files (x86)\Graphviz2.38\bin路径下, 也可以考虑将该路径加入到环境变量中。
同样, 为了方便处理批量的.dot文件, 可以将以上命令写成批处理的形式。
以下是批处理的一个简单样例, 请根据实际情况修改：

    .. code-block:: bash

       del d:\Z-output\* /Q
       for %%i in (*.dot) do
           d:\"Program Files (x86)"\Graphviz2.38\bin\dot.exe %%i -Tpng -o d:\Z-output\%%i.png

3.4 生成的Plan可视化图样例
~~~~~~~~~~~~~~~~~~~~~~~~~~~

    .. image:: static/plan_pic_example_1.png
       :width: 1000px


    .. image:: static/plan_pic_example_2.png
       :width: 1000px


    .. image:: static/plan_pic_example_3.png
       :width: 1000px
