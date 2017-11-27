==================
开发者新手入门教程
==================

本章是一个新手入门的教程(tutorial), 让你快速了解Flume.


编译并运行
----------

申请开发机
~~~~~~~~~~
Flume的代码只能在百度的开发机上编译通过, 所以就不要在自己的机器上折腾了, 自己的笔记本,
或创建一个虚拟机, 都是徒劳的.

申请一个开发机, 登陆relay跳板机(relay01.baidu.com), 再从relay跳板机上登陆开发机, 就可以开始干活了.

步骤如下：

1.申请开发机
去 http://ocean.baidu.com 申请开发机, 设置一下samba的密码,
然后可以把这台linux开发机的硬盘和你的Windows笔记本共享

2.共享文件夹
将你的Linux开发机的文件夹与你的Windows笔记本共享, 这样你可以在Windows上修改代码, 在Linux里编译.

申请svn密码并申请权限
~~~~~~~~~~~~~~~~~~~~~
flume使用git+blade+gerrit+JenKins工作流框架(http://wiki.babel.baidu.com/twiki/bin/view/Com/Test/GitGerritJenkinsBlade)进行开发，在使用该工作流前请确保你拥有相关权限。

权限可以在:(http://icafe.baidu.com/?#myrights/rights/showRights)进行查看。

如果没有权限，请按照以下步骤进行获取：

获取svn密码
###########
登录 http://spm.work.baidu.com/， 点击Change svn password，设置密码。

获取blade权限
#############
登录 http://spm.work.baidu.com/ 点击 code access permission application，在path中选择blade，账号输入你的邮箱前缀，勾选write，以及所有可选选项。

安装必要的软件包
~~~~~~~~~~~~~~~
为配置flume开发环境，需要配置下载安装一些别要的软件包

安装jumbo
#########
Jumbo是百度自己的包管理工具, 类似于Ubuntu上的apt和CentOS上的yum, 区别是jumbo不需要root权限,
更多信息见官网 http://jumbo.baidu.com/ .
    .. code-block:: shell

        bash -c "$( curl http://jumbo.baidu.com/install_jumbo.sh )"; source ~/.bashrc

安装git
#######
    .. code-block:: shell

        jumbo install git

安装gcc 4.8.2
#############
Flume的代码需要gcc 4.8.2, 为确定可以正常运行，先查看gcc版本：
    .. code-block:: shell

        gcc --version

如果得到的不是gcc(GCC)4.8.2则查看 /opt/compiler/ 下文件夹是否存在4.8.2版本gcc，如果不存在，去http://bpkg.baidu.com/找到gcc4.8.2.且必须要安装到/opt/compiler/, 安装命令如下：

    .. code-block:: shell

        wget http://bpkg.baidu.com/gcc-4.8.2/gcc-4.8.2.11-installer.bin
        sudo sh gcc-4.8.2.11-installer.bin /opt/compiler/gcc-4.8.2


运行后，更改根目录下.bashrc 配置，注意要将export行添加到 #.bashrc 行下

    .. code-block:: shell

        vim ~/.bashrc
        export PATH=/opt/compiler/gcc-4.8.2/bin:$PATH
        source ~/.bashrc

clone代码
~~~~~~~~~
首先要生成一对公钥和私钥, 然后把公钥在gerrit里登记. 先生成一对一对公钥和私钥,

    .. code-block:: shell

        ssh-keygen -C daifangqin@baidu.com -t rsa    #注意邮箱换成你自己的, 按几下回车
        nano ~/.ssh/id_rsa.pub     #复制里面的所有内容

访问 http://git.inf.baidu.com:8081/#/settings/ssh-keys , 将公钥在网页里添加.

配置一下git信息（可选）
    .. code-block:: shell

        git config --global user.name ${USER}
        git config --global user.email ${USER}@baidu.com
        git config --global github.user ${USER}@baidu.com

clone code库文件到本地

    .. code-block:: shell

        git clone ssh://${USER}@git.scm.baidu.com:8235/blade



编译
~~~~
注意, 百度的编译工具使用的是腾讯的blade, 但是各个项目组略有修改, 本项目使用的blade, 跟
'jumbo install blade'里的不同, 所以不要使用jumbo里的blade, 而要安装本项目自带的blade:
    .. code-block:: shell

        cd blade/blade
        ./install
        jumbo install scons   # blade 底层使用了scons
        cd ..

首次编译需要运行prepare.sh script，后面可以直接blade build，每当有大版本更新再运行prepare.sh即可

    .. code-block:: shell

        ./prepare.sh flume
        blade build flume

运行单元测试
~~~~~~~~~~~~
去 http://dpfhelp.dmop.baidu.com/ 下载 hadoop-client 1.2.4, 然后解压, 设置JAVA_HOME, 将bin加入path,
然后"blade test ..."即可.
   .. code-block:: shell

        wget -O hadoop-client.tar.gz http://koala.dmop.baidu.com:8080/fc/getfilebyid?id=4826
        tar zxf hadoop-client.tar.gz -C ~/local/opt
        vim ~/.bashrc
        export JAVA_HOME=$HOME/local/opt/hadoop-client/java6
        export HADOOP_HOME=$HOME/local/opt/hadoop-client/hadoop
        export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH

要把 libjvm.so 加入到 LD_LIBRARY_PATH,
    .. code-block:: shell

        vim ~/.bashrc
        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/jre/lib/amd64/server

运行所有测试用例,
    .. code-block:: shell

        cd ~/local/src/baidu/flume
        blade test ...

正常情况下应该全部通过, 如果出现红色的failed情况, 请务必一条一条消除错误.

在公司的Hadoop集群上运行任务
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

配置Hadoop Client
#################
向公司的集群提交任务, 需要先去 http://dpfhelp.dmop.baidu.com/ 申请一个账号,
并配置到 hadoop-site.xml 里面. 好在团队里有一个配置的好的 hadoop-site.xml , 内容如下：
    .. code-block:: xml

        <?xml version="1.0"?>
        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

        <configuration>

        <!-- NEEDED TO CHANGE -->

        <property>
          <name>hadoop.job.ugi</name>
          <value>root,root</value>
          <description>username, password used by client</description>
        </property>

        <property>
          <name>fs.default.name</name>
          <value>hdfs://cq01-odsp-q3zf003bak.cq01.baidu.com:34275</value>
          <description>The name of the default file system.  A URI whose
          scheme and authority determine the FileSystem implementation.  The
          uri's scheme determines the config property (fs.SCHEME.impl) naming
          the FileSystem implementation class.  The uri's authority is used to
          determine the host, port, etc. for a filesystem.</description>
        </property>

        <property>
          <name>mapred.job.tracker</name>
          <value>cq01-odsp-q3zf003bak.cq01.baidu.com:62717</value>
          <description>The host and port that the MapReduce job tracker runs
          at.  If "local", then jobs are run in-process as a single map
          and reduce task.
          </description>
        </property>

        </configuration>

覆盖掉原始的 hadoop-site.xml , 并设置HADOOP_HOME环境变量, 就这样就配置好了.

接下来验证客户端, 首先查看一下版本,
    .. code-block:: shell

       hadoop version

用dfs命令浏览一下文件,
    .. code-block:: shell

       hadoop dfs -ls /

向集群提交任务
##############
    .. code-block:: shell

        cd ~/local/src/baidu/flume/tests
        alt
        GLOG_logtostderr=1
        ./dce_io

加 GLOG_logtostderr=1 的意思是让日志打印到屏幕上.

运行 wordcount 这个最经典的例子,
    .. code-block:: shell

        cd ~/local/src/baidu/flume/tests
        alt
        ./dce_wordcount

在HDFS上查看结果,
    .. code-block:: shell

        hadoop dfs -ls /user/wenxiang/output

根据创建时间, 猜测自己的输出在哪个目录, 因为程序是刚刚跑完的, 所以目录创建时间就在几分钟之内.
    .. code-block:: shell

        bin/hadoop dfs -cat /user/wenxiang/output/output-0


使用Eclipse CDT阅读源码
-----------------------
由于代码在开发机上, 只能使用vim阅读, 效率很低, 因此, 要想办法把代码下载到本地, 再用eclipse来阅读.

用http协议将代码git clone下载下来, 不能用SSH, 因为网关那里限制了IP,
只有开发机能后用SSH协议下载代码, 但是http协议没有这个限制,
   .. code-block:: shell

       git clone http://git.inf.baidu.com:8081/baidu

大小大概有1G多, 下载后, 可以导入到elcipse CDT. 注意, 由于thirdparty 里都是第三方包, 尤其是boost,
代码特别多, eclipse在建立索引时会分析这些代码, 导致eclipse非常卡, 几个小时都没有响应,
可以把boost删除, 速度立马变快. 如果你的电脑内存足够大, 也可以不删.

在Eclipse里点击菜单"File->New->Makefile project with existing code", Project name 填写为"baidu",
Existing Code Location 为刚刚git clone 下来的目录, ToolChain for Indexer Settings选择"Linux GCC",
最后点击"Filish"按钮即可. 这时Eclipse开始分析代码, 建立索引, 要等待很长时间.


核心概念
--------
整体的工作流程是：用户使用flume api创建逻辑计划(LogicalPlan), LogicalPlan是通过ApiPlanBuilder里面,
记录pcollection的groupByKey()等api调用而形成的, 通过 LocalPlanner.Plan()生成PbPhysicalPlan,
存成一个文件, 然后由 LocalBackend::Execute() 读取该文件, 开始执行.

总体流程是：逻辑计划 -> Planner -> 物理计划 -> runtime .

逻辑计划是上层的概念, Planner是一个优化器o, 用于一遍一遍的遍历整个图, 做一些优化,
比如删除不必要的节点等, 然后输出成物理计划, 后端得到物理计划后就可以运行.
基本上一个物理计划对应着一个后端.

逻辑计划由scope和node组成, scope是一棵树, 叶子节点才是node, 各个node组成一个dag,
因此逻辑计划是scope组成的树+node组成的DAG组成.

物理计划只由executor组成, 是一个DAG.


更多详细信息请参考 :doc:`core`