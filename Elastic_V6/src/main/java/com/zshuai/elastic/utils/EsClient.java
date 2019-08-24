package com.zshuai.elastic.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class EsClient {
	static TransportClient client = null;
	/**  
	 * <p>Title: getEsClient</p>  
	 * <p>Description: 获取elastic的client</p>  
	 * @param name：elastic的cluster.name
	 * @param host：elastic的主机地址
	 * @return
	 * @throws UnknownHostException  
	 */  
	@SuppressWarnings("unchecked")
	public static TransportClient getEsClient(String name,String host) throws UnknownHostException {
		 Settings settings = Settings.builder().put("cluster.name", name).build();
	        client = new PreBuiltTransportClient(settings);
	        client = client.addTransportAddress(new TransportAddress(InetAddress.getByName(host), 9300));
	        System.out.println(client.toString());
	        return client;
	}

}
