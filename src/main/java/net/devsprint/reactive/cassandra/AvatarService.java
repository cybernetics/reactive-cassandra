package net.devsprint.reactive.cassandra;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import rx.Observable;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class AvatarService {
	private static final String QUERY = "select * avatars";
	private static final ExecutorService executorService = Executors
			.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

	private final Session session;

	public AvatarService(Session session) {
		this.session = session;
	}

	Observable<ResultSet> getAvatars() {
		return ObservableCassandra
				.executeAsync(session, QUERY, executorService);
	}

}
