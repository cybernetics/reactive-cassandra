package net.devsprint.reactive.cassandra;

import rx.Observable;
import rx.Subscription;
import rx.util.functions.Action1;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

/**
 * Reactive cassandra ...
 * 
 */
public class ReactiveCassandra {

	public static void main(String[] args) throws InterruptedException {
		ReactiveCassandra cas = new ReactiveCassandra();
		AvatarService service = new AvatarService(cas.getSession());
		Subscription subscription = service.getAvatars().subscribe(
				new Action1<ResultSet>() {

					@Override
					public void call(ResultSet result) {
						Observable<Row> rows = Observable.from(result.all());
						rows.subscribe(new Action1<Row>() {

							@Override
							public void call(Row row) {
								System.out.println("Row: " + row.toString());

							}

						});
					}

				});
		subscription.unsubscribe();

	}

	private Session getSession() {
		final int concurency = 8;
		int maxConnections = 10;

		PoolingOptions pools = new PoolingOptions();
		pools.setMaxSimultaneousRequestsPerConnectionThreshold(
				HostDistance.LOCAL, concurency);
		pools.setCoreConnectionsPerHost(HostDistance.LOCAL, maxConnections);
		pools.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections);
		pools.setCoreConnectionsPerHost(HostDistance.REMOTE, maxConnections);
		pools.setMaxConnectionsPerHost(HostDistance.REMOTE, maxConnections);
		Cluster cluster = new Cluster.Builder()
				.addContactPoints("localhost").withPoolingOptions(pools)
				.withSocketOptions(new SocketOptions().setTcpNoDelay(true))
				.build();

		return cluster.connect();
	}
}
