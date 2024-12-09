//package serizalier
//
//import com.esotericsoftware.kryo.Kryo
//import com.esotericsoftware.kryo.Serializer
//import com.esotericsoftware.kryo.io.{Input, Output}
//import java.nio.ByteBuffer
//
//class ByteBufferSerializer extends Serializer[ByteBuffer] {
//  override def write(kryo: Kryo, output: Output, buffer: ByteBuffer): Unit = {
//    val bytes = new Array[Byte](buffer.remaining())
//    buffer.get(bytes)
//    output.writeInt(bytes.length)
//    output.writeBytes(bytes)
//  }
//
//  override def read(kryo: Kryo, input: Input, t: Class[ByteBuffer]): ByteBuffer = {
//    val length = input.readInt()
//    val bytes = input.readBytes(length)
//    ByteBuffer.wrap(bytes)
//  }
//}
