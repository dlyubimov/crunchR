package org.crunchr.fn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.crunchr.types.RType;

/**
 * Unfortunately we have to create a specific functional treatement of grouped
 * do functions (functions that specifically run on the reducer input).
 * <P>
 * 
 * The problem here is that Crunch (as well as Hadoop, in fact) is using
 * pull-style value type of Pair<K,Iterable<V>>. However, we cannot fully
 * serialize the Iterable<V> without a risk of putting out a very long messages
 * which require a great amount of memory. So we have to chunkify Iterable<V>
 * transmission into several messages. Since these messages, in general, are not
 * guaranteed to be adjacent in the R queues, we cannot use pull interface and
 * in fact will have a different functional set up.
 * <P>
 * 
 * And of course, as i mentioned, we cannot treat value of grouped function as
 * indivisible entity which means it cannot be serialized as a regular single
 * RType instance to the R side. Hence, a specific treatment is required.
 * <P>
 * 
 * @author dmitriy
 * 
 * @param <K>
 * @param <V>
 * @param <T>
 */
public class RGroupedDoFn<K, V, T> extends RDoFn<Pair<K, Iterable<V>>, T> {

    private static final Logger s_log            = Logger.getLogger("crunchR");

    private static final long   serialVersionUID = 1L;

    /*
     * TODO: obviously item size in a chunk may vary a lot. For something like
     * matrix blocks (somethings that i use a lot) even 20 items may represent a
     * big memory chunk. So we need to be able to override this.
     */
    private int                 chunkSize        = 20;
    private transient List<V>   valueChunk;

    /**
     * deserializing factory method
     * 
     * @return doFn
     */
    @SuppressWarnings("unchecked")
    public static <K, V, T> RGroupedDoFn<K, V, T> fromBytes(byte[] bytes) throws IOException {
        return (RGroupedDoFn<K, V, T>) RDoFnRType.getInstance()
            .get(ByteBuffer.wrap(bytes), new RGroupedDoFn<K, V, T>());
    }

    @Override
    public void initialize() {
        try {

            init();
            rpipe.addGroupedDoFn(this);

        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    public RType<Pair<K, Iterable<V>>> getGroupedSRType() {
        return srtype;
    }

    @Override
    public void process(Pair<K, Iterable<V>> src, Emitter<T> emitter) {
        try {
            this.emitter = emitter;

            /*
             * So with grouped data, we don't send the whole group as a single
             * value since group can be, well, quite unbounded. So our
             * serialized value is a "group chunk".
             * 
             * The protocol convention goes that first chunk contains key (and
             * flagged as "first group chunk" ) and subseqent chunks contain
             * only values, until group is depleted.
             * 
             * The last chunk is also flagged as a "last chunk".
             * 
             * Also notice that since the DoFn graph may cause additional
             * emissions, which can in their turn generate additional
             * dispatches, the group chunks are not necessarily consecutively
             * ordered in the pipe. Hence, grouping on the R side is inevitably
             * a push API -- unlike pull API on the Crunch side of things.
             * 
             * The restriction of a push API on the R side potentially could be
             * lifted if we could guarantee the whole task graph execution on
             * the R side, which is ideal but not the case at this point. As of
             * the time of this writing, IntermediateEmitter is still invoked on
             * the Crunch side and may cause additional dispatches interrupting
             * a reducer group.
             */

            if (valueChunk == null)
                valueChunk = new ArrayList<V>(chunkSize);

            boolean firstChunk = true;

            for (Iterator<V> viter = src.second().iterator(); viter.hasNext();) {
                valueChunk.clear();
                for (int count = 0; count < chunkSize && viter.hasNext(); count++) {
                    valueChunk.add(viter.next());
                }
                Pair<K, Iterable<V>> holder =
                    firstChunk ? Pair.of(src.first(), (Iterable<V>) valueChunk) : Pair.of((K) null,
                                                                                          (Iterable<V>) valueChunk);

//                s_log.info("sending " + valueChunk.size() + " items to R.");

                rpipe.add(holder, getGroupedSRType(), doFnRef);
                /*
                 * after the first chunk, the convention is that we don't
                 * serialize the key anymore.
                 */
                if (holder.first() != null)
                    holder = Pair.of(null, holder.second());
            }

            if (valueChunk.size() > 0) {
                /* send terminator with 0 size */
                valueChunk.clear();
                rpipe.add(Pair.of((K) null, (Iterable<V>) valueChunk), getGroupedSRType(), doFnRef);
            }
        } catch (IOException exc) {
            // TODO: so what are we supposed to wrap it to in Crunch?
            throw new RuntimeException(exc);
        }

    }
}
