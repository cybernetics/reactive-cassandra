/**
 * 
 */
package net.devsprint.reactive.cassandra;

import java.util.concurrent.ExecutorService;

import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.subscriptions.Subscriptions;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

/**
 * Wraps an async execution of Datastax Java driver into an observable.
 * 
 */
public class ObservableCassandra {
	
	public static Observable<ResultSet> executeAsync(final Session execution,
			final String query, final ExecutorService executorService) {
		return Observable.create(new Observable.OnSubscribeFunc<ResultSet>() {

			@Override
			public Subscription onSubscribe(
					final Observer<? super ResultSet> observer) {
				try {
					Futures.addCallback(execution.executeAsync(query),
							new FutureCallback<ResultSet>() {

								@Override
								public void onSuccess(ResultSet result) {
									observer.onNext(result);
									observer.onCompleted();
								}

								@Override
								public void onFailure(Throwable t) {
									observer.onError(t);
								}
							}, executorService);
				} catch (Throwable e) {
					// If any Throwable can be thrown from
					// executeAsync
					observer.onError(e);
				}
				return Subscriptions.empty();
			}
		});
	}

}
