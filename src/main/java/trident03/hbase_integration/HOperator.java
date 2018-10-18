package trident03.hbase_integration;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * HOperator
 * @author kinglyjn
 * @date 2018年8月14日
 *
 */
public class HOperator implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(HOperator.class);
	
	
	/**
	 * 创建表空间
	 * 
	 */
	public void createNamespace(String namespaceName) {
		Admin admin = null;
		try {
			admin = HConnectionUtils.getAdmin();
			boolean namespaceExists = false;
			NamespaceDescriptor[] nsds = admin.listNamespaceDescriptors();
			for (NamespaceDescriptor nsd : nsds) {
				if (namespaceName.equals(nsd.getName())) {
					namespaceExists = true;
					break;
				}
			}
			if (namespaceExists) {
				LOGGER.error("表空间 {} 已经存在，无法创建", namespaceName);
				return;
			}
			NamespaceDescriptor desc = NamespaceDescriptor.create(namespaceName).build();
			admin.createNamespace(desc);
		} catch (IOException e) {
			LOGGER.error("表空间 {} 创建失败", namespaceName);
			e.printStackTrace();
		} finally {
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 删除表空间
	 * 
	 */
	public void dropNamespace(String namespaceName) {
		Admin admin = null;
		try {
			admin = HConnectionUtils.getAdmin();
			boolean namespaceExists = false;
			NamespaceDescriptor[] nsds = admin.listNamespaceDescriptors();
			for (NamespaceDescriptor nsd : nsds) {
				if (namespaceName.equals(nsd.getName())) {
					namespaceExists = true;
					break;
				}
			}
			if (!namespaceExists) {
				LOGGER.info("表空间 {} 不存在", namespaceName);
				return;
			}
			TableName[] tableNames = admin.listTableNamesByNamespace(namespaceName);
			if (tableNames!=null && tableNames.length!=0) {
				LOGGER.info("表空间 {} 中存在表，不能删除", namespaceName);
				return;
			}
			admin.deleteNamespace(namespaceName);
		} catch (IOException e) {
			LOGGER.error("表空间 {} 失败", namespaceName);
			e.printStackTrace();
		} finally {
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 列出所有表空间
	 * 
	 */
	public NamespaceDescriptor[] listNamespace() {
		NamespaceDescriptor[] nsds = null;
		Admin admin = null;
		try {
			admin = HConnectionUtils.getAdmin();
			nsds = admin.listNamespaceDescriptors();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return nsds;
	}
	
	/**
	 * 列出某个表空间的所有表
	 * 
	 */
	public TableName[] listNamespaceTables(String namespace) {
		TableName[] tableNames = null;
		Admin admin = null;
		try {
			admin = HConnectionUtils.getAdmin();
			tableNames = admin.listTableNamesByNamespace(namespace);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return tableNames;
	}
	
	
	/**
	 * 创建表<br>
	 * 表名、列族、版本、存活时间<br><br>
	 * 
	 * 注意：<br>
	 * 1. 如果minVersions、maxVersions其中一个或全为空，或者minVersions>maxVersions则使用hbase默认版本<br>
	 * 2. timeToLive单位为秒。timeToLive若为空，则使用hbase默认超时时间，通常和minVersions参数一起使用<br>
	 * 
	 */
	public void createTable(String tableName, String columnFamily, 
			Integer minVersions, Integer maxVersions,Integer timeToLive) {
		Admin admin = null;
		try {
			// 判断
			admin = HConnectionUtils.getAdmin();
			TableName tb = TableName.valueOf(tableName);
			if (admin.tableExists(tb)) {
				LOGGER.info("表 {} 已经存在，无法创建", tableName);
				admin.close();
			    return;
			} 
			
			// 设置表的属性
			HTableDescriptor desc = new HTableDescriptor(tb);
			//desc.setMaxFileSize(512); //设置一个region中的store文件的最大size，当达到这个size时，region就开始分裂
			//desc.setMemStoreFlushSize(512); //设置region内存中的memstore的最大值，当memstore达到这个值时，开始往磁盘中刷数据
			//
			//由于HBase的数据是先写入内存，数据累计达到内存阀值时才往磁盘中flush数据，所以如果在数据还
			//没有flush进硬盘时，regionserver down掉了，内存中的数据将丢失。要想解决这个场景的问题就
			//需要用到WAL预写日志。该方式安全性较高，但无疑会一定程度影响性能，请根据具体场景选择使用。
			//该设置可以在相关的三个对象中使用，即 HTableDescriptor、Put、Delete。
			//desc.setDurability(Durability.SYNC_WAL); 
			
			// 设置列族的属性
			HColumnDescriptor hcolumnDescriptor = new HColumnDescriptor(columnFamily);
			hcolumnDescriptor.setBlockCacheEnabled(true); //默认开启读缓存
			hcolumnDescriptor.setInMemory(true); //
			if (minVersions!=null && maxVersions!=null && minVersions>=0 && maxVersions>0 && minVersions<=maxVersions) {
				hcolumnDescriptor.setMinVersions(minVersions);
				hcolumnDescriptor.setMaxVersions(maxVersions);
			}
			if (timeToLive != null ) {
				hcolumnDescriptor.setTimeToLive(timeToLive);
			}
			desc.addFamily(hcolumnDescriptor);
			
			// 创建表
			admin.createTable(desc);
		} catch (Exception e) {
			LOGGER.info("表 {} 创建失败", tableName);
			e.printStackTrace();
		} finally {
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 删除表
	 * 
	 */
	public void dropTable(String tableName) {
		Admin admin = null;
		try {
			admin = HConnectionUtils.getAdmin();
			TableName tb = TableName.valueOf(tableName);
			if (!admin.tableExists(tb)) {
				LOGGER.info("表 {} 不存在，无法删除", tableName);
				admin.close();
			    return;
			} 
			if (!admin.isTableDisabled(tb)) {
				admin.disableTable(tb);
			}
			admin.deleteTable(tb);
		} catch (Exception e) {
			LOGGER.error("表 {} 删除失败", tableName);
			e.printStackTrace();
		} finally {
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	} 
	
	
	/**
	 * putOne数据
	 * 
	 */
	public void putOne(String tableName, Put put) {
		Table table = null;
		try {
			table = HConnectionUtils.getTable(tableName);
			table.put(put);
		} catch (Exception e) {
			LOGGER.error("putOne失败, tableName={}, put={}", tableName, put);
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * putList数据
	 * 
	 */
	public void putList(String tableName, List<Put> puts) {
		Table table = null;
		try {
			table = HConnectionUtils.getTable(tableName);
			table.put(puts);
		} catch (Exception e) {
			LOGGER.error("putList失败, tableName={}, puts={}", tableName, puts);
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	/**
	 * 检查更新<br>
	 * value：是与服务端check的预期值，只有服务器端对应rowkey的数据与你预期的值相同时，你的put操作才能被提交到服务端执行<br>
	 * put：是你需要录入的数据的put对象<br>
	 * 
	 */
	public boolean checkAndPut(String tableName, String rowkey, String columnFamily, 
			String column, CompareOp compareOp, byte[] value, Put put) {
		boolean checkAndPut = false;
		Table table = null;
		try {
			table = HConnectionUtils.getTable(tableName);
			checkAndPut = table.checkAndPut(Bytes.toBytes(rowkey), Bytes.toBytes(columnFamily), Bytes.toBytes(column), compareOp, value, put);
		} catch (Exception e) {
			LOGGER.error("checkAndPut失败, tableName={}, rowkey={}, columnFamily={}, clolumn={}", tableName, rowkey, columnFamily, column);
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return checkAndPut;
	}
	
	/**
	 * 递增计数器
	 * 注意：列的类型必须为long类型
	 * 
	 */
	public Long increment(String tableName, String rowkey, 
			String columnFamily, String column, Long step) {
		Table table = null;
		Long increCount = null;
		try {
			table = HConnectionUtils.getTable(tableName);
			increCount = table.incrementColumnValue(Bytes.toBytes(rowkey), Bytes.toBytes(columnFamily), Bytes.toBytes(column), step); 
		} catch (Exception e) {
			LOGGER.error("increment失败, tableName={}, rowkey={}, columnFamily={}, column={}, step={}", 
					tableName, rowkey, columnFamily, column, step);
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return increCount;
	}
	
	/**
	 * 删除数据
	 * 
	 */
	public void deleteOne(String tableName, Delete delete) {
		Table table = null;
		try {
			table = HConnectionUtils.getTable(tableName);
			table.delete(delete);
		} catch (Exception e) {
			LOGGER.error("deleteRow失败, tableName={}, delete={}", tableName, delete);
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 删除多行数据
	 * 
	 */
	public void deleteList(String tableName, List<Delete> deletes) {
		Table table = null;
		try {
			table = HConnectionUtils.getTable(tableName);
			table.delete(deletes);
		} catch (Exception e) {
			LOGGER.error("deleteRows失败, tableName={}, deletes={}", tableName, deletes);
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 检查删除数据
	 * 
	 */
	public boolean checkAndDelete(String tableName, String rowkey, String columnFamily, 
			String column, CompareOp compareOp, byte[] value, Delete delete) {
		boolean checkAndDelete = false;
		Table table = null;
		try {
			table = HConnectionUtils.getTable(tableName);
			checkAndDelete = table.checkAndDelete(Bytes.toBytes(rowkey), Bytes.toBytes(columnFamily), Bytes.toBytes(column), compareOp, value, delete);
		} catch (Exception e) {
			LOGGER.error("checkAndDelete失败, tableName={}, rowkey={}, columnFamily={}, clolumn={}", tableName, rowkey, columnFamily, column);
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return checkAndDelete;
	}
	
	/**
	 * 判断数据是否存在
	 * 
	 */
	public boolean exists(String tableName, String rowkey) {
		boolean exists = false;
		Table table = null;
		try {
			table = HConnectionUtils.getTable(tableName);
			Get get = new Get(Bytes.toBytes(rowkey));
			exists = table.exists(get);
		} catch (IOException e) {
			LOGGER.error("exists出现错误, tableName={}, rowkey={}", tableName, rowkey);
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return exists;
	}
	
	/**
	 * get数据
	 * 
	 */
	public Result[] get(String tableName, List<Get> gets) {
		Table table = null;
		Result[] results = null;
		try {
			table = HConnectionUtils.getTable(tableName);
			results = table.get(gets);
			
			//get
			/*Get get = new Get(Bytes.toBytes("1,199,912"));
			Result result = table.get(get);
			get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
			get.setTimeRange(minStamp, maxStamp);
			get.setTimeStamp(timestamp)
			Cell[] rawCells = result.rawCells();
			for (Cell cell : rawCells) {
				String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell)); //注意需要类型匹配，不然Bytes转化的可能是错误类型的数据
				String column = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				System.out.println(columnFamily + ":" + column + " -> " + value);
			}*/
		} catch (IOException e) {
			LOGGER.error("get数据失败, tableName={}, gets={}", tableName, gets);
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return results;
	}
	
	/**
	 * scan数据
	 * 
	 */
	public ResultScanner scan(String tableName, Scan scan) {
		Table table = null;
		ResultScanner scanner = null;
		try {
			table = HConnectionUtils.getTable(tableName);
			//scan
			/*Scan scan = new Scan();
			scan.setRaw(false); 	//原生扫描（能够扫描到KEEP_DELETED_CELLS的记录）
			scan.setCaching(1000); 	//设置缓存行的大小（服务器每次返回的行数）
			scan.setBatch(10); 		//设置批量列的大小（服务器每次返回的列数）
			Filter rf = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("1,000"))); //百万级记录filter耗时24s
			scan.setFilter(rf);
			ResultScanner resultScanner = table.getScanner(scan);
			for (Result result : resultScanner) {
				...
			}*/
			scanner = table.getScanner(scan);
		} catch (Exception e) {
			LOGGER.error("scan失败, tableName={}, scan={}", tableName, scan);
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return scanner;
	}
	
	/**
	 * scan数据，根据 PrefixFilter(rowkey前缀必须匹配才能查到)
	 * 
	 */
	public ResultScanner scan(String tableName, String rowkeyLike) {
		Table table = null;
		ResultScanner scanner = null;
		try {
			table = HConnectionUtils.getTable(tableName);
			//
			PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes(rowkeyLike));
			Scan scan = new Scan();
			scan.setFilter(prefixFilter);
			//
			scanner = table.getScanner(scan);
		} catch (IOException e) {
			LOGGER.error("get数据失败, tableName={}, rowkeyLike={}", tableName, rowkeyLike);
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return scanner;
	}
	
	/**
	 * 查询行数
	 * 
	 */
	public Long rowCount(String tableName) {
		Table table = null;
		long rowCount = 0;
		try {
			table = HConnectionUtils.getTable(tableName);
			Scan scan = new Scan();
			scan.setFilter(new FirstKeyOnlyFilter());
			ResultScanner resultScanner = table.getScanner(scan);
			for (Result result : resultScanner) {  
	            rowCount += result.size();  
	        }  
		} catch (Exception e) {
			LOGGER.error("rowCount失败, tableName={}", tableName);
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return rowCount;
	}
	
	
	
	public static void main(String[] args) {
		//HOperator operator = new HOperator();
		
		// 创建表空间
		//operator.createNamespace("test01");
		
		// 删除表空间
		//operator.dropNamespace("test02");
		
		// 列出表空间
		//NamespaceDescriptor[] namespaces = operator.listNamespace();
		//Arrays.asList(namespaces).forEach(System.out::println);
		
		// 列出表空间中的所有表
		//TableName[] tables = operator.listNamespaceTables("test01");
		//Arrays.asList(tables).forEach(System.out::println);
		
		// 创建表
		//operator.createTable("test01:t2", "cf", 1, 3, 60);
		
		// 删除表
		//operator.dropTable("test01:t2");
		
		// putOne
		//Put put = new Put(Bytes.toBytes("row1"));
		//put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
		//operator.putOne("test01:t1", put);
		
		// putList
		/*Put put1 = new Put(Bytes.toBytes("row2"));
		put1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
		put1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(25));
		Put put2 = new Put(Bytes.toBytes("row3"));
		put2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("xiaojuan"));
		put2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(21));
		operator.putList("test01:t1", Arrays.asList(put1, put2));*/
		
		// checkAndPut
		/*Put put3 = new Put(Bytes.toBytes("row3"));
		put3.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("xiaojuan2"));
		boolean checkAndPut = operator.checkAndPut("test01:t1", "row3", "cf1", "name", CompareOp.EQUAL, Bytes.toBytes("xiaojuan"), put3);
		System.out.println(checkAndPut);*/
		
		// increment
		//Long increment = operator.increment("test01:t1", "row2", "cf1", "age", 1L);
		//System.out.println(increment);
		
		// deleteOne
		//Delete delete = new Delete(Bytes.toBytes("row4"));
		//operator.deleteOne("test01:t1", delete);
		
		// deleteList
		/*Delete delete2 = new Delete(Bytes.toBytes("row2"));
		Delete delete3 = new Delete(Bytes.toBytes("row3"));
		operator.deleteList("test01:t1", Arrays.asList(delete2, delete3));*/
		
		// checkAndDelete
		
		// exists
		//boolean exists = operator.exists("test01:t1", "row2");
		//System.out.println(exists);
		
		// get
		/*Get get = new Get(Bytes.toBytes("row2"));
		List<Get> gets = Arrays.asList(get);
		Result[] results = operator.get("test01:t1", gets);
		for (Result result : results) {
			String name = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name")));
			int age = Bytes.toInt((result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("age"))));
			System.out.println(name + " " + age);
		}*/
		
		// scan
		/*Scan scan = new Scan(Bytes.toBytes("row1"));
		ResultScanner scanner = operator.scan("test01:t1", scan);
		for (Result result : scanner) {
			String name = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name")));
			int age = Bytes.toInt((result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("age"))));
			System.out.println(name + " " + age);
		}*/
		
		/*ResultScanner scanner = operator.scan("test01:t1", "row");
		for (Result result : scanner) {
			String name = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name")));
			int age = Bytes.toInt((result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("age"))));
			System.out.println(name + " " + age);
		}*/
		
		// rowCount
		/*Long rowCount = operator.rowCount("test01:t1");
		System.out.println(rowCount);*/
	}
	
}
