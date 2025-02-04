package com.comcast.xfinity.sirius.api;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

/**
 * An implementation of {@link RequestHandler} that supports parallelized bootstrapping by dropping events that have an
 * older sequence number for a given key than events that have already been processed.
 *
 * @param <K> the key value
 * @param <M> the deserialized message
 */
public abstract class AbstractParallelBootstrapRequestHandler<K, M> implements RequestHandler {
    private ConcurrentMap<K, Long> latestSequencePerKey = null;

    @Override
    public final void onBootstrapStarting() {
        onBootstrapStarting(false);
    }

    @Override
    public final void onBootstrapStarting(boolean parallel) {
        if (parallel) {
            latestSequencePerKey = new ConcurrentHashMap<>();
        }
        onBootstrapStartingImpl(parallel);
    }

    @Override
    public final void onBootstrapComplete() {
        latestSequencePerKey = null;
        onBootstrapCompletedImpl();
    }

    @Override
    public SiriusResult handleGet(String key) {
        return readsEnabled() ? handleGetImpl(createKey(key)) : SiriusResult.none();
    }

    @Override
    public SiriusResult handlePut(String key, byte[] body) {
        return writesEnabled() ? handlePutImpl(createKey(key), deserialize(body)) : SiriusResult.none();
    }

    @Override
    public SiriusResult handlePut(long sequence, String key, byte[] body) {
        if (!writesEnabled()) {
            return SiriusResult.none();
        }
        K k = createKey(key);
        ConcurrentMap<K, Long> latestSequencePerKey = this.latestSequencePerKey;
        if (latestSequencePerKey == null) {
            return handlePutImpl(sequence, k, deserialize(body));
        }
        Long existing = latestSequencePerKey.get(k);
        if (existing != null && existing > sequence) {
            // bail early
            return SiriusResult.none();
        }

        PutUpdateFunction f = new PutUpdateFunction(sequence, deserialize(body));
        latestSequencePerKey.compute(k, f);
        return f.result();
    }

    @Override
    public SiriusResult handleDelete(String key) {
        return writesEnabled() ? handleDeleteImpl(createKey(key)) : SiriusResult.none();
    }

    @Override
    public SiriusResult handleDelete(long sequence, String key) {
        if (!writesEnabled()) {
            return SiriusResult.none();
        }
        K k = createKey(key);
        ConcurrentMap<K, Long> latestSequencePerKey = this.latestSequencePerKey;
        if (latestSequencePerKey == null) {
            return handleDeleteImpl(sequence, k);
        }

        DeleteUpdateFunction f = new DeleteUpdateFunction(sequence);
        latestSequencePerKey.compute(k, f);
        return f.result();
    }

    protected boolean readsEnabled() {
        return true;
    }

    protected boolean writesEnabled() {
        return true;
    }

    protected abstract K createKey(String key);
    protected abstract M deserialize(byte[] body);

    protected void onBootstrapStartingImpl(boolean parallel) { }
    protected void onBootstrapCompletedImpl() { }
    protected abstract SiriusResult handleGetImpl(K key);
    protected abstract SiriusResult handlePutImpl(K key, M body);
    protected abstract SiriusResult handleDeleteImpl(K key);

    protected SiriusResult handlePutImpl(long sequence, K key, M body) {
        return handlePutImpl(key, body);
    }

    protected SiriusResult handleDeleteImpl(long sequence, K key) {
        return handleDeleteImpl(key);
    }

    private final class PutUpdateFunction implements BiFunction<K, Long, Long> {
        private final long sequence;
        private final M body;
        private SiriusResult result = SiriusResult.none();

        public PutUpdateFunction(long sequence, M body) {
            this.sequence = sequence;
            this.body = body;
        }

        public SiriusResult result() {
            return result;
        }

        @Override
        public Long apply(K key, Long existing) {
            if (existing != null && existing > sequence) {
                return existing;
            }
            result = handlePutImpl(sequence, key, body);
            return sequence;
        }
    }

    private final class DeleteUpdateFunction implements BiFunction<K, Long, Long> {
        private final long sequence;
        private SiriusResult result = SiriusResult.none();

        public DeleteUpdateFunction(long sequence) {
            this.sequence = sequence;
        }

        public SiriusResult result() {
            return result;
        }

        @Override
        public Long apply(K key, Long existing) {
            if (existing != null && existing > sequence) {
                return existing;
            }
            result = handleDeleteImpl(sequence, key);
            return sequence;
        }
    }
}
