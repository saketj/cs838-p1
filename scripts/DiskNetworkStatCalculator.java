import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DiskNetworkStatCalculator {
	public static void main(String args[]) throws IOException {
		FileReader a = new FileReader(args[0]);
		FileReader b = new FileReader(args[1]);
		BufferedReader in1 = new BufferedReader(a);
		BufferedReader in2 = new BufferedReader(b);
		long sectorsRead = 0;
		long sectorsWritten = 0;
		long packetsReceived = 0;
		long packetsTransmitted = 0;
		int vmCount = 0;
		for(int i = 0; i < 24; i++) {//each line
			String[] lineFile1 = in1.readLine().trim().split(" +");
			String[] lineFile2 = in2.readLine().trim().split(" +");
			if(i%2 == 1) {
				continue; //skip odd lines
			}
			if(i%6==0) {
				vmCount++;
			}
			long newSectorsRead = 0;
			long newSectorsWritten = 0;
			long currentSectorsRead = Long.parseLong(lineFile2[5]);
			long previousSectorsRead = Long.parseLong(lineFile1[5]);
			long currentSectorsWritten = Long.parseLong(lineFile2[9]);
			long previousSectorsWritten = Long.parseLong(lineFile1[9]);
			newSectorsRead = currentSectorsRead - previousSectorsRead;
			newSectorsWritten = currentSectorsWritten - previousSectorsWritten;
			System.out.println("VM" + vmCount + " " + lineFile1[2] + "-" + lineFile2[2]);
			System.out.println("Previous Sectors Read = " + previousSectorsRead);
			System.out.println("Current Sectors Read = " + currentSectorsRead);
			System.out.println("Previous Sectors Written = " + previousSectorsWritten);
			System.out.println("Current Sectors Written = " + currentSectorsWritten);

			System.out.println("Sectors Read = " + newSectorsRead);
			System.out.println("Sectors Written = " + newSectorsWritten);
			System.out.println("--------------------");
			sectorsRead += newSectorsRead;
			sectorsWritten +=newSectorsWritten;
		}
		vmCount = 1;
		for(int i = 0; i < 4; i++) {
			String[] lineFile1 = in1.readLine().trim().split(" +");
			String[] lineFile2 = in2.readLine().trim().split(" +");
			long previousPacketsReceived = Long.parseLong(lineFile1[1]);
			long currentPacketsReceived = Long.parseLong(lineFile2[1]);
			long previousPacketsTransmitted = Long.parseLong(lineFile1[9]);
			long currentPacketsTransmitted = Long.parseLong(lineFile2[9]);
			long newPacketsReceived = currentPacketsReceived - previousPacketsReceived;
			long newPacketsTransmitted = currentPacketsTransmitted - previousPacketsTransmitted;
			System.out.println("VM" + vmCount);
			System.out.println("Previous Packets Received = " + previousPacketsReceived);
			System.out.println("Current Packets Received = " + currentPacketsReceived);
			System.out.println("Previous Packets Transmitted = " + previousPacketsTransmitted);
			System.out.println("Current Packets Transmitted = " + currentPacketsTransmitted);
			System.out.println("Packets received = " + newPacketsReceived);
			System.out.println("Packets Transmitted = " + newPacketsTransmitted);
			System.out.println("--------------------");
			packetsReceived += newPacketsReceived;
			packetsTransmitted += newPacketsTransmitted;
			vmCount++;
		}
		System.out.println("Total Sectors Read = " + sectorsRead*512);
		System.out.println("Total Sectors Written = " + sectorsWritten*512);
		System.out.println("Total Packets Received = " + packetsReceived);
		System.out.println("Total Packets Transmitted = " + packetsTransmitted);
		in1.close();
		in2.close();
	}
}
