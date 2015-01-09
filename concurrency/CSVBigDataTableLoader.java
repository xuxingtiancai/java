/**
 * Load the csv file and convert each line to a row.
 * using multiThread
 * 
 * @author xinxu
 */
public class CSVBigDataTableLoaderNew extends CSVBigDataTableLoader {	
	private static final Log LOG = LogFactory
			.getLog(CSVBigDataTableLoaderNew.class);
	
	//sentry
	private final static byte[] ReaderSentry = new byte[0];
	private final static List<Object> ParserSentry = Lists.newArrayList();
	
	//buffer
	private final BlockingQueue<Chunk<byte[]>> readerBuffer;
	private final BlockingQueue<Chunk<List<Object>>> parserBuffer;
	
	//chunkSize
	private final int readerChunkSize;
	private final int parserChunkSize;
	
	//executor
	private final int parserThreadSize;
	private final ExecutorService executor;
	private final ScheduledThreadPoolExecutor executorTracker;
	
	//next() (final fetch data)
	private Chunk<List<Object>> resultChunk;
	private int parserSentryCounter;
	
	//chunk (buffer store type)
	class Chunk<E> {
		private List<E> content;
		private int cursor;
		public Chunk() {
			this.content = Lists.newArrayList();
			this.cursor = 0;
		}
		public void add(E e) {
			content.add(e);
		}
		public E get(int index) {
			return content.get(index);
		}
		public int size() {
			return content.size();
		}
		
		public void begin() {
			cursor = 0;
		}
		public E next() {
			if(cursor == size()) {
				return null;
			}
			return content.get(cursor++);
		}
	}
	//reader
	class Reader implements Runnable {
		public void run() {		
			Chunk<byte[]> chunk = new Chunk<byte[]>();
			try {
				// Skip first #numSkipLines rows
				while (numSkipLines-- > 0 && reader.nextKeyValue()) {
				}

				while (reader.nextKeyValue()) {
					Text text = (Text) reader.getCurrentValue();
					byte[] bytes = text.copyBytes();
					
					//Chunk
					chunk.add(bytes);
					if(chunk.size() == readerChunkSize) {
						readerBuffer.put(chunk);
						chunk = new Chunk<byte[]>();
					}
				}
				
				//buffer
				for(int i = 0; i < parserThreadSize; i++) {
					//Chunk
					chunk.add(ReaderSentry);
					readerBuffer.put(chunk);
					chunk = new Chunk<byte[]>();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}
	//parser
	class Parser implements Runnable {
		private final CSVParser parser;
		public Parser(CSVParser parser) {
			this.parser = parser;
		}
		public void run() {
			try {
				Chunk<List<Object>> outputChunk = new Chunk<List<Object>>();
				while (true) {
					Chunk<byte[]> inputChunk = readerBuffer.take();
					for(int chunkInd = 0; chunkInd < inputChunk.size(); chunkInd++) {
						byte[] bytes = inputChunk.get(chunkInd);

						if (bytes == ReaderSentry) {
							//chunk
							outputChunk.add(ParserSentry);
							parserBuffer.put(outputChunk);
							return;
						}
						String line = new String(bytes, charset);
						List<String> strValues = parser.parseLine(line);

						if (strValues == null || strValues.size() == 0) {
							// If we read nothing, say blank line, just skip it.
							// TQMS 942670l
							continue;
						}

						List<Object> values = Lists.newArrayList();
						for (int i = 0; i < index.size(); i++) {
							int pos = index.get(i);
							values.add(pos < strValues.size() ? typeConverters
									.get(i).convert(strValues.get(pos)) : null);
						}

						//chunk
						outputChunk.add(values);
						if(outputChunk.size() == parserChunkSize) {
							parserBuffer.put(outputChunk);
							outputChunk = new Chunk<List<Object>>();
						}
					}
				}
			} catch (TypeMismatchException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}
	//tracker
	class Tracker implements Runnable {
		public void run() {
			printBuffer();
		}
	}

	public CSVBigDataTableLoaderNew(BaseTable table,
			List<? extends BaseColumn> columns, Optional<String> delimiter,
			Optional<String> offsets, RecordReader<?, ?> reader, JobConf conf)
			throws TypeMismatchException {
		this(table, columns, delimiter, offsets, 0, Optional
				.<Character> absent(), reader, EncodingUtils.DEFAULT_ENCODING, conf);
	}

	public CSVBigDataTableLoaderNew(BaseTable table,
			List<? extends BaseColumn> columns, Optional<String> delimiter,
			Optional<String> offsets, int numSkipLines,
			Optional<Character> quoteChar, RecordReader<?, ?> reader,
			String encoding, JobConf conf) throws TypeMismatchException {
		super(table, columns, delimiter, offsets, numSkipLines, quoteChar, reader, encoding);
		
		//buffer
		this.readerBuffer = new LinkedBlockingQueue<Chunk<byte[]>>(
				conf.getInt("com.microstrategy.bde.load.CSVBigDataTableLoader.readerBufferSize", 50));
		this.parserBuffer = new LinkedBlockingQueue<Chunk<List<Object>>>(
				conf.getInt("com.microstrategy.bde.load.CSVBigDataTableLoader.parserBufferSize", 50));
		
		//next
		this.parserSentryCounter = 0;
		this.resultChunk = null;
		
		//chunkSize
		this.readerChunkSize =
				conf.getInt("com.microstrategy.bde.load.CSVBigDataTableLoader.readerChunkSize", 100);
		this.parserChunkSize =
				conf.getInt("com.microstrategy.bde.load.CSVBigDataTableLoader.parserChunkSize", 100);
		
		//thread
		this.parserThreadSize = 
				conf.getInt("com.microstrategy.bde.load.CSVBigDataTableLoader.parserThreadSize", 3);
		this.executor = Executors.newFixedThreadPool(this.parserThreadSize + 1,
                ThreadUtil.newNamedThreadFactory(this.getClass().getSimpleName()));
		executor.submit(new Reader());
		for(int parserInd = 0; parserInd < parserThreadSize; parserInd++) {
			CSVParser parser = quoteChar.isPresent() ? //
					new CSVParser(delimiter.or(CSV_LOADER_DEFAULT_DELIMITER).charAt(0),
							quoteChar.get(), true) //
							: new CSVParser(delimiter.or(CSV_LOADER_DEFAULT_DELIMITER)
									.charAt(0), true);
			executor.submit(new Parser(parser));
		}
		LOG.info("CSVBigDataTableLoader executor create multiple threads for parse parserThreadSize=" + this.parserThreadSize);
		
		//tracker
		executorTracker = new ScheduledThreadPoolExecutor(1);
		//executorTracker.scheduleAtFixedRate(new Tracker(), 0, 100, TimeUnit.MILLISECONDS);
		
		//the first chunk
		try {
			resultChunk = parserBuffer.take();
			resultChunk.begin();
		} catch (InterruptedException e) {
			throw ThrowableUtil.propagate(e);
		}
	}

	@Override
	public List<Object> next() {
		List<Object> result = null;
		try {
			while(true) {
				//read result
				while(true) {
					result = resultChunk.next();
					if (result == null) {
						resultChunk = parserBuffer.take();
						resultChunk.begin();
					} else {
						break;
					}
				}
				//check result
				if (result == ParserSentry) {
					parserSentryCounter++;
					if (parserSentryCounter == parserThreadSize) {
						executor.shutdownNow();
						executorTracker.shutdownNow();
						return null;
					}
				} else {
					break;
				}
			}	
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	
	//debug: print loader buffer
	public void printBuffer() {
		System.out.println("DEBUG:bufferInfo: reader=" + readerBuffer.size() + 
				" parser=" + parserBuffer.size());
	}
}
