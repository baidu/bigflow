############
开发环境
############

Bigflow的开发工作完全按照层次做解耦，不同层之间通过Pb进行计划的序列化/反序列化交互，
按照层次划分的

Bigflow API：与具体的语言有关，这里涉及Python/C++

Bigflow Core：

相关软件及硬件

项目规范
==============

* 代码规范

  对于C++/Python，要求符合公司的代码规范

  对于Scala，公司尚无统一规范，因此与官方规范相一致(http://docs.scala-lang.org/style/)


* 单元测试

  GTest(C++)
  unittest(Python)
  ScalaTest(Scala)


* 构建工具

  Blade(C++)
  Maven(Scala)


* 开发工具
  无要求
