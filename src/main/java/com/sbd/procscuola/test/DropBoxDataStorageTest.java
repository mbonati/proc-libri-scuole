package com.sbd.procscuola.test;

import java.io.File;

import com.sbd.procscuola.storage.DataStorageClient;
import com.sbd.procscuola.storage.impl.dropbox.DropBoxDataStorage;

public class DropBoxDataStorageTest {

	public static void main(String[] args) throws Exception {
		
		String folderPath = "/Users/marcobonati/Develop/sources/Personal/sbd_lista_scuole/proc-libri-scuole/output";
		
		DataStorageClient dbxs = new DropBoxDataStorage();
		
		dbxs.setup(null);
		
		dbxs.uploadFolder(new File(folderPath));

	}

}
