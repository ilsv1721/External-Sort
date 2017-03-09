import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Representation of Big data file.
 * 
 * @author ilya
 *
 */
public class BigDataFile extends File {

	private static final long serialVersionUID = 4948461744793957642L;

	/**
	 * MAXIMUM POSIBLE number of temporary files during external sort. Default
	 * is 2048;
	 */
	private final int maxTempFilesExternalSort;
	static private final int DEFFAULT_MAX_FILES = 2048;

	/*
	 * Approximate value of String overhead in JAVA.
	 * 
	 */
	static private final int ApproxStringJVMOverhead = 75;

	public BigDataFile(String filePath) {
		super(filePath);
		maxTempFilesExternalSort = DEFFAULT_MAX_FILES;
	}

	public BigDataFile(String filePath, int maxTempFilesForSort) {
		super(filePath);
		maxTempFilesExternalSort = maxTempFilesForSort;
	}

	/**
	 * Additional task 1. Finding a string which contains substring.
	 * 
	 * @param substring
	 * @param outp
	 * @throws IOException
	 */
	public void findStringWithSubstring(String substring, PrintStream outp) throws IOException {
		try (Stream<String> stream = Files.lines(Paths.get(getPath()))) {
			stream.forEach(str -> {
				if (str.contains(substring)) {
					outp.println(str);
				}
			});
		}
	}

	/**
	 * Additional task 1. Finding a string with matched substring using RegEx.
	 * 
	 * @param substring
	 * @param outp
	 * @throws IOException
	 */

	public void findStringRegEx(Pattern pattern, PrintStream outp) throws IOException {

		try (Stream<String> stream = Files.lines(Paths.get(getPath()))) {
			stream.forEach(str -> {
				Matcher m = pattern.matcher(str);
				if (m.find()) {
					outp.println(str);
				}
			});
		}
	}

	/**
	 * External sort with natural compatator
	 * 
	 * @param tempFilesPrefix
	 *            prefix for temporary files
	 * @param tempFilesSuffix
	 *            suffix for temporary files
	 * @param outputFileName
	 *            file to output sorted data
	 * @throws IOException
	 */
	public void externalSort(String tempFilesPrefix, String tempFilesSuffix, String outputFileName) throws IOException {
		externalSort(tempFilesPrefix, tempFilesSuffix, outputFileName, Comparator.naturalOrder());
	}

	/**
	 * External sort with natural compatator.
	 * 
	 * @param tempFilesPrefix
	 *            prefix for temporary files
	 * @param tempFilesSuffix
	 *            suffix for temporary files
	 * @param outputFileName
	 *            file to output sorted data
	 * 
	 * @throws IOException
	 */
	public void externalSort(String tempFilesPrefix, String tempFilesSuffix, String outputFileName,
			Comparator<String> comp) throws IOException {

		List<File> tempfiles = new LinkedList<>();
		long tempfilesize = calcSizeOfTempExternalFile();
		PriorityQueue<String> priorityQueue = new PriorityQueue<>(comp);

		// Creating temporary files with chunks of sorted data

		try (BufferedReader bufferedReader = new BufferedReader(new FileReader(getPath()))) {
			String readedline = "";
			while (readedline != null) {
				long currenttempfilesize = 0;
				while ((readedline = bufferedReader.readLine()) != null && currenttempfilesize < tempfilesize) {
					currenttempfilesize += readedline.length() * Character.BYTES + ApproxStringJVMOverhead;
					priorityQueue.add(readedline);
				}
				File tmpFile = File.createTempFile(tempFilesPrefix, tempFilesSuffix);
				tmpFile.deleteOnExit();
				tempfiles.add(tmpFile);

				try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(tmpFile))) {
					while (!priorityQueue.isEmpty()) {
						bufferedWriter.write(priorityQueue.poll());
						bufferedWriter.newLine();
					}
					bufferedWriter.flush();
				}
			}
		}

		// merging the sorted temporary files

		PriorityQueue<LastLineManipulationBufferReader> manipMerg = new PriorityQueue<>((first, second) -> {
			return comp.compare(first.peek(), second.peek());
		});

		for (File f : tempfiles) {
			LastLineManipulationBufferReader lstMBu = new LastLineManipulationBufferReader(
					new BufferedReader(new FileReader(f)));
			manipMerg.add(lstMBu);
		}

		try (BufferedWriter fbw = new BufferedWriter(new FileWriter(outputFileName))) {
			while (!manipMerg.isEmpty()) {
				LastLineManipulationBufferReader lastlinemanip = manipMerg.poll();
				String stringToFile = lastlinemanip.pop();
				fbw.write(stringToFile);
				fbw.newLine();
				if (lastlinemanip.peek() != null) {
					manipMerg.add(lastlinemanip);
				} else {
					lastlinemanip.close();
				}

			}

		}
	}

	// calculating optimal size for temporary files
	private long calcSizeOfTempExternalFile() {
		long tempfilesize = length() / maxTempFilesExternalSort;
		Runtime runtime = Runtime.getRuntime();
		runtime.gc();
		long availablememory = runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory());

		if (tempfilesize < availablememory / 1.5)
			tempfilesize = (long) (availablememory / 1.5);
		else {
			throw new RuntimeException("High risk of running out of RAM memory during external sort");
		}
		return tempfilesize;
	}

	// Helper class with ability to get cached String from last readln
	class LastLineManipulationBufferReader extends Reader {

		private String cache;
		private final BufferedReader bufferedReader;

		public LastLineManipulationBufferReader(BufferedReader bufferedReader) throws IOException {
			this.bufferedReader = bufferedReader;
			cache = bufferedReader.readLine();
			if (cache == null)
				throw new RuntimeException("File contains no data");
		}

		public String peek() {
			return cache;
		}

		public String pop() throws IOException {
			String returnedString = cache;
			cache = bufferedReader.readLine();
			return returnedString;
		}

		@Override
		public int read(char[] cbuf, int off, int len) throws IOException {
			return bufferedReader.read(cbuf, off, len);
		}

		@Override
		public void close() throws IOException {
			bufferedReader.close();

		}

	}

}
