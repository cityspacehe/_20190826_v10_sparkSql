package com.cityspace;

/**
 * @author hq
 * @date 2019-08-28
 */
/**
 * 工厂模式就是对实现同一接口的多个类进行统一的实例创建
 */

public class FactoryTest {
    public static void main(String[] args) {
        // 方法一 普通模式
        SendFactory factory=new SendFactory();
        Sender sender=factory.product("sms");
        sender.send();
        // 方法二 ： 多个工厂方法模式
        SendFactory1 factory1=new SendFactory1();
        Sender sender1=factory1.produceMail();
        sender1.send();
        // 方法三： 静态工厂方法模式
        // 将上面的工厂方法改为静态的  从而不用创建对象

    }

}


interface Sender{
    void send();
}
class MailSender implements Sender{

    @Override
    public void send() {
        System.out.println("this is mailSender class");
    }
}

class SmsSender implements Sender{

    @Override
    public void send() {
        System.out.println("this is SmsSender class");
    }
}

/**
 * 工厂类  同一创建实列对象
 *  用户输入有误的情况不能创建对象
 */
class SendFactory{
    public Sender product(String type){
        if("mail".equals(type)){
            return new MailSender();
        }
        if("sms".equalsIgnoreCase(type)){
            return new SmsSender();
        }else {
            System.out.println("输入的类型有误");
            return null;
        }
    }
}

/**
 * 多个工厂创建对象的模式
 */
class SendFactory1{
    public Sender produceMail(){
        return new MailSender();
    }
    public Sender produceSms(){
        return new SmsSender();
    }
}


