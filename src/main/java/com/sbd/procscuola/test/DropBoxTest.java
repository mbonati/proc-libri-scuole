package com.sbd.procscuola.test;

import java.io.File;
import java.io.FileInputStream;
import java.util.Locale;

import com.dropbox.core.DbxAppInfo;
import com.dropbox.core.DbxClient;
import com.dropbox.core.DbxEntry;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.DbxWebAuthNoRedirect;
import com.dropbox.core.DbxWriteMode;

public class DropBoxTest {

	public static void main(String[] args) throws Exception {
		
		 // Get your app key and secret from the Dropbox developers website.
        final String APP_KEY = "tb1d6ixzxd2pomi";
        final String APP_SECRET = "s8xtrwvn7si8rbu";

        DbxAppInfo appInfo = new DbxAppInfo(APP_KEY, APP_SECRET);

        DbxRequestConfig config = new DbxRequestConfig(
            "JavaTutorial/1.0", Locale.getDefault().toString());
        DbxWebAuthNoRedirect webAuth = new DbxWebAuthNoRedirect(config, appInfo);
        
        String authorizeUrl = webAuth.start();
        System.out.println("1. Go to: " + authorizeUrl);
        System.out.println("2. Click \"Allow\" (you might have to log in first)");
        System.out.println("3. Copy the authorization code.");
        //String code = new BufferedReader(new InputStreamReader(System.in)).readLine().trim();
        //mXP6cC_TOsoAAAAAAAAAuOS6lONLDFCKfZBcN4LzkTo
        
        String code = "mXP6cC_TOsoAAAAAAAAAuu1Mmg86UhHug8OTNt_k2h0";
        
        //DbxAuthFinish authFinish = webAuth.finish(code);
        //String accessToken = authFinish.accessToken;
        
        String accessToken = "mXP6cC_TOsoAAAAAAAAAvNS20k9IyfGOMQVanjFD5wEwnqkzRj_eAvKB514GpLCM";
        
        DbxClient client = new DbxClient(config, accessToken);
        System.out.println("Linked account: " + client.getAccountInfo().displayName);
        
        
        File inputFile = new File("pom.xml");
        FileInputStream inputStream = new FileInputStream(inputFile);
        try {
            DbxEntry.File uploadedFile = client.uploadFile("/pom.xml",
                DbxWriteMode.add(), inputFile.length(), inputStream);
            System.out.println("Uploaded: " + uploadedFile.toString());
        } finally {
            inputStream.close();
        }
	}

}
