package jabbersimulator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableList;
import com.sangupta.murmur.Murmur3;

import trd.test.jabbersimulator.Generator;
import trd.test.utilities.Utilities;

public class ArrowWriteTest {
	
    private static Schema makeArrowSchema(){
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("platform",       FieldType.nullable(new ArrowType.Int(32, true)), null));
        childrenBuilder.add(new Field("orgId",          FieldType.nullable(new ArrowType.Binary()), null));
        childrenBuilder.add(new Field("productName",    FieldType.nullable(new ArrowType.Int(32, true)), null));
        childrenBuilder.add(new Field("productVersion", FieldType.nullable(new ArrowType.Int(32, true)), null));
        childrenBuilder.add(new Field("customerid",     FieldType.nullable(new ArrowType.Int(64, true)), null));
        childrenBuilder.add(new Field("type",           FieldType.nullable(new ArrowType.Int(32, true)), null));
        childrenBuilder.add(new Field("installationId", FieldType.nullable(new ArrowType.Binary()), null));
        childrenBuilder.add(new Field("time",           FieldType.nullable(new ArrowType.Int(64, true)), null));
        return new Schema(childrenBuilder.build(), null);
    }
    
	final static String[] fieldNameList = new String[] { 
			"platform",
			"orgId",
			"productName",
			"productVersion",
			"customerid",
			"type",
			"installationId",
			"time"
	};
	
	public static String getGuidFromByteArray(byte[] bytes) {
	    ByteBuffer bb = ByteBuffer.wrap(bytes);
	    long high = bb.getLong();
	    long low = bb.getLong();
	    UUID uuid = new UUID(high, low);
	    return uuid.toString();
	}

	public static void main(String[] args) throws IOException {
		String arrowFile = "/tmp/foo.arrow";
		Schema schema = makeArrowSchema();

        RootAllocator ra = new RootAllocator(Integer.MAX_VALUE);
		VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, ra);        	
		try (FileOutputStream fos = new FileOutputStream(arrowFile)) {			
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
	        try (ArrowFileWriter aw = new ArrowFileWriter(vsr, provider, fos.getChannel())) {
				aw.start();

				for (int batchId = 0; batchId < 10; batchId++) {	
					vsr.setRowCount(10);
					for (int i = 0; i < 10; i++) {
						UUID installationIdUuid = UUID.randomUUID();
						byte[] installationId = Utilities.getUUIDAsByte(installationIdUuid);
			
						// Get the Field Vectors and update
						((IntVector)vsr.getVector("platform")).setSafe(i, i);
						
						byte[] orgId = Utilities.getUUIDAsByte(UUID.randomUUID());
						VarBinaryVector varBinaryVector = (VarBinaryVector)vsr.getVector("orgId");
			            varBinaryVector.setIndexDefined(i);
			            varBinaryVector.setValueLengthSafe(i, orgId.length);					
			            varBinaryVector.setSafe(i, orgId);
						
						((IntVector)vsr.getVector("productName")).setSafe(i, i);
						((IntVector)vsr.getVector("productVersion")).setSafe(i, i);
						((BigIntVector)vsr.getVector("customerid")).setSafe(i, i);
						((IntVector)vsr.getVector("type")).setSafe(i, i);
						
						varBinaryVector = (VarBinaryVector)vsr.getVector("installationId");
						varBinaryVector.setIndexDefined(i);
			            varBinaryVector.setValueLengthSafe(i, installationId.length);					
			            varBinaryVector.setSafe(i, installationId);
			            varBinaryVector.setValueCount(varBinaryVector.getValueCount() + 1);
			            
						((BigIntVector)vsr.getVector("time")).setSafe(i, new Date().getTime());
					}
					
					for (String fieldName : fieldNameList) {
						vsr.getVector(fieldName).setValueCount(10);
					}					
					aw.writeBatch();

					System.out.printf("Before Vector cleaning: %d\n", ra.getAllocatedMemory());
					// Truncate the vectors
					for (String field: fieldNameList) {
						vsr.getVector(field).clear();
						vsr.getVector(field).setInitialCapacity(10);
						vsr.getVector(field).allocateNew();
					}
					System.out.printf("After  Vector cleaning: %d\n", ra.getAllocatedMemory());
				}
				aw.end();
			}
			fos.flush();
		}
		
		int count = 0;
		try (FileInputStream fis = new FileInputStream(arrowFile)) {
			DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
			try (ArrowFileReader arrowFileReader = new ArrowFileReader(new SeekableReadChannel(fis.getChannel()), ra)) {
				VectorSchemaRoot root = arrowFileReader.getVectorSchemaRoot();
				List<ArrowBlock> arrowBlocks = arrowFileReader.getRecordBlocks();
				for (int i = 0; i < arrowBlocks.size(); i++) {
					ArrowBlock ab = arrowBlocks.get(i);
					arrowFileReader.loadRecordBatch(ab);
					
					System.out.printf("Block---------------%d\n", i);
					List<FieldVector> fieldVector = root.getFieldVectors();
					
					for (int j = 0; j < fieldVector.size(); j++) {
						FieldVector fv = fieldVector.get(j);
						String fieldName = fv.getField().getName();
						int fc = fv.getValueCount();
						if (j == 6) {
							VarBinaryVector varBinaryVector = ((VarBinaryVector) fv);
							for (int k = 0; k < fc; k++) {
								byte[] bVal = varBinaryVector.get(k);
								System.out.printf("%s\n", getGuidFromByteArray(bVal));
							}
						}
//						System.out.printf("%s:%d\n", fieldName, fc);
					}
				}
			}
		}
		System.out.printf("Readback %d rows\n", count);
	}
}
