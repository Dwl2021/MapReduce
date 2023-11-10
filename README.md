# MapReduce

这是一个利用go语言实现分布式MapReduce的项目。主要内容是将里面的8个txt文件中的所有单词通过多协程的方式统计各个单词的数目最后得到输出。

## 快速开始

新启动一个终端，运行工作分配者。

```shell
git clone http://github.com:Dwl2021/MapReduce.git
cd src/main
sh coordinator.sh
```

然后再另起一个终端，运行工作者。

```shell
cd src/main
sh worker.sh
```

