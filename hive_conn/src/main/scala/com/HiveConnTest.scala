package com

import java.sql.DriverManager

/**
  *
  * @author hq
  * @date 2019-08-31
  *
  */
object HiveConnTest extends App {
  val url="jdbc:hive2://192.168.130.111:10000/works"
//  jdbc:hive2://192.168.130.111:10000/works";
  Class.forName("org.apache.hive.jdbc.HiveDriver")
  val conn=DriverManager.getConnection(url,"","")
  val sql="select *from score"
  val ps=conn.prepareStatement(sql)
  val res=ps.executeQuery()
  while(res.next()){
    val tno=res.getString(1)
    val sno=res.getString(2)
    val score=res.getString(3)
    println(tno+" "+sno+" "+score)
  }

}
