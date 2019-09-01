package com.cityspace;

/**
 * @author hq
 * @date 2019-08-28
 *
 *
 * 适配器模式分为 类、对象、接口的适配器模式
 *
 * 适配器模式将某个类的接口转换成客户端期望的另一个接口表示，
 * 目的是消除由于接口不匹配所造成的类的兼容性问题。
 *
 *
 * 类的适配器模式：当希望将一个类转换成满足另一个新接口的类时，可以使用类的适配器模式，创建一个新类，继承原有的类，实现新的接口即可。
 *
 * 对象的适配器模式：当希望将一个对象转换成满足另一个新接口的对象时，可以创建一个Wrapper类，持有原类的一个实例，在Wrapper类的方法中，调用实例的方法就行。
 *
 * 接口的适配器模式：当不希望实现一个接口中所有的方法时，可以创建一个抽象类Wrapper，实现所有方法，我们写别的类的时候，继承抽象类即可
 */
public class AdapterTest {
    public static void main(String[] args) {
        // 类适配测试
        Targetable t=new Adapter();
        t.method1();
        t.method2();

        // 对象适配测试
        Source source=new Source();
        Targetable target=new Wrapper(source);
        target.method2();
        target.method1();

        // 接口适配测试

        Sourceable s1=new SourceSub1();
        Sourceable s2=new SourceSub2();
        s1.method1();
        s1.method2();
        s1.method3();
        s2.method1();
        s2.method2();
        s2.method3();


    }

}

/**
 * 类的适配
 * 有一个Source类，拥有一个方法，待适配，目标接口时Targetable，
 * 通过Adapter类，将Source的功能扩展到Targetable里
 * 这样Targetable接口的实现类就具有了Source类的功能。
 */
class Source{
    public void method1(){
        System.out.println("this is origial methos");
    }
}

interface Targetable{
    void method1(); // 与原类中方的方法同
    void method2(); // 新类中的方法
}

class Adapter extends Source implements Targetable{

    @Override
    public void method2() {
        System.out.println("this is the targetable methos");
    }
}

/**
 * 对象的适配
 * 与类适配差不多，只是将Adapter 修改
 * 这次不继承Source类，
 * 而是持有Source类的实例，以达到解决兼容性的问题。
 */

class Wrapper implements Targetable{
    private Source source;
    public Wrapper(Source source){
        this.source=source;
    }
    @Override
    public void method1() {
        source.method1();
    }

    @Override
    public void method2() {
        System.out.println("this is the mubiao method");
    }
}


/**
 *  *接口的适配
 *  * 原始接口中有多个抽象方法，
 *  * 当我们写该接口的实现类时，
 *  * 必须实现该接口的所有方法，
 *  * 这明显有时比较浪费，
 *  * 有时只需要某一些，引入了接口的适配器模式
 *  * 借助于一个抽象类，
 *  * 该抽象类实现了该接口，实现了所有的方法，
 *  * 而我们不和原始的接口打交道，只和该抽象类取得联系，
 *  * 所以我们写一个类，继承该抽象类，重写我们需要的方法就行。
 */

interface  Sourceable{
    void method1();
    void method2();
    void method3();
}

// 抽象类有非抽象方法
abstract class Wrapper2 implements Sourceable{
    @Override
    public void method2() {

    }
    @Override
    public void method1() {

    }
    @Override
    public void method3() {

    }
}

class SourceSub1 extends Wrapper2{
   @Override
    public void method2(){
       System.out.println("this is subMethod2");
    }
}
class SourceSub2 extends Wrapper2{
    @Override
    public void method1(){
        System.out.println("this is subMethod1");
    }
}