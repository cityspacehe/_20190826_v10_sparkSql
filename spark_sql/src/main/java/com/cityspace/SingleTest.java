package com.cityspace;

/**
 * @author hq
 * @date 2019-08-28
 */

/**
 * 单列模式： 保证jvm中  该对象只有一个
 *
 *1、某些类创建对象比较频繁，系统开销比较大
 * 2、省区了new 操作符 降低了系统内存的使用频率，减轻了GC压力
 * 3、交易所核心交易引擎。只能出现一个对象控制整个交易流程

 单列模式  首先构造方法私有化
 */
public class SingleTest {

}

class Singleton{
    private Singleton(){}

    // 持有私有的静态实列，防止被引用，此处赋值为null 目的是延迟加载
    private static Singleton instance=null;

    /**
     *这样的方式性能有一定的改善，但是可能会出现
     * 空对象的异常
     *
     * instance=new Singleton();
     * 上面的语句分两步执行，但是不保证先后顺序
     * 如果先划分内存空间然后再对其实列化  那么会出现错误
     *
     * @return
     */
    public static Singleton getInstance(){
        if(instance==null){
            synchronized(instance){
                if(instance==null){
                    instance=new Singleton();
                }
            }
        }
        return instance;
    }

    /**
     * 这样的方法对线程是比较安全  只是在每次getinstance
     * 的时候都需要对对象上锁，
     * 事实上只需要第一次创建对象的时候上锁即可
     * @return
     */
    public static synchronized Singleton getInstance1(){
        if(instance==null){
            instance=new Singleton();
        }
        return instance;
    }


    /**
     * 通过内部类的方式创建 JVM内部的机制能够保证当一个类被加载的时候，
     * 这个类的加载过程是线程互斥的。
     *
     *如果再构造函数中出现异常，那么对象将永远不会被创建
     *
     */

    private static class SingletonFactory{
        private static Singleton instance=new Singleton();
    }
    public static Singleton getInstance3(){
        return SingletonFactory.instance;
    }


    /**
     * 以下这样的分开方式也行
     */
    private static synchronized void syncInit(){
        if(instance==null){
            instance=new Singleton();
        }
    }
    public static Singleton getInstance5(){
        if(instance==null){
            syncInit();
        }
        return instance;
    }


    /**
     * 如果该对象被用于序列化，可以保证对象在序列化前后一致
      * return
     */
    public Object readResolve(){
        return instance;
    }

}


/**
 * 饿汉模式的单列
  */
class Single{
    private Single(){}

    private static Single instance=new Single();
    public static Single getInstance(){
        return instance;
    }
}