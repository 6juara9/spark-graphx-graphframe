//package serizalier
//
//import com.esotericsoftware.kryo.Kryo
//import org.apache.spark.serializer.KryoRegistrator
//
//import java.nio.ByteBuffer
//
//class MyKryoRegistrator extends KryoRegistrator {
//  override def registerClasses(kryo: Kryo): Unit = {
//    kryo.register(classOf[ByteBuffer], new ByteBufferSerializer)
//    // Реєструйте інші класи, якщо необхідно
//  }
//}