package org.crunchr.r;

import java.io.IOException;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.crunchr.io.TwoWayRPipe;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;

/**
 * In order R to startup in-proc (including server side),
 * 
 * <ol>
 * 
 * <LI>Install R
 * <LI>install <code>rJava</code> R package
 * <LI>add result of <code>R --vanilla <<< 'system.file("jri", package="rJava")</code> to
 * LD_LIBRARY_PATH of the mapreduce user (and driver user too), seems to work.
 * Alternatively, for the driver you may use the old one -Djava.library.path,
 * and for the the hadoop nodes you could configure the same for the task
 * command lines. (I like universal setup instructions with LD_LIBRARY_PATH
 * easier.) Yet another option for hadoop nodes is possibly soft-link the libjre.so
 * to Hadoop's native libs folder.
 * 
 * </ol>
 * 
 * 
 * @author dmitriy
 * 
 */
public class RController extends Configured {

    private static final String R_ARGS_PROP = "crunchr.rargs";

    private static RController  singleton;
    private Rengine             rengine;
    private String              rhome;
    private TwoWayRPipe         rpipe;

    private RController() {
        super();
    }

    private void initialize() throws IOException {

        rhome = System.getenv("R_HOME");
        if (rhome == null)
            throw new IOException("R_HOME is not set.");

        Configuration conf = getConf();

        String args[] = conf.get(R_ARGS_PROP, "--vanilla").split(" ");

        rengine = new Rengine(args, false, null);
        rpipe = new TwoWayRPipe(1 << 12);

        /*
         * load library
         */
        REXP r = rengine.eval("library(crunchR)");
        if (r == null)
            throw new IOException("Unable to load R package crunchR.");

        /*
         * setup error handling
         */
        rengine.eval("options(error=quote(dump.frames(\"errframes\", F)))");

    }

    public Object eval(String expr) throws RCallException {

        REXP res = rengine.eval(expr.toString());
        if (res == null) {
            REXP frames = rengine.eval("names(errframes)");
            String error = rengine.eval("geterrmessage()").toString();

            throw new RCallException(String.format("Evaluation failed, error:\n %s; frames:\n%s\n ",
                                                   error,
                                                   frames.toString()));
        }
        return simplify(res);

    }

    /**
     * Very slow way to call R side. We only use it for task setups. it is not
     * suitable for frequent iterations (instead, we bulk up requests and make
     * rJava call from the R side instead.)
     * 
     * @param methodName
     * @param args
     * @return
     * @throws RCallException
     */
    public Object evalWithMethodArgs(String methodName, Object... args) throws RCallException {
        Validate.notNull(rengine);

        REXP rargs = rengine.createRJavaRef(args);

        rengine.assign("crunchR.rcargs__", rargs);
        rengine.eval("crunchR.rcargs__ <- .jevalArray(crunchR.rcargs__);");

        StringBuffer eval = new StringBuffer(methodName);
        eval.append("(");

        for (int i = 1; i <= args.length; i++) {
            if (args[i - 1] == null)
                eval.append("NULL");
            else
                eval.append(String.format(".jsimplify(crunchR.rcargs__[[%d]])", i));
            if (i < args.length)
                eval.append(",");
        }
        eval.append(")");
        Object r = eval(eval.toString());
        eval("rm(crunchR.rcargs__)");
        return r;

    }

    public Rengine getEngine() {
        return rengine;
    }

    public TwoWayRPipe getRPipe() {
        return rpipe;
    }

    /**
     * I don't make it thread safe here since most of our tasks (backend, or
     * driver) will be single threaded in context of FlumeJava.
     * 
     * 
     * @return
     */
    public static RController getInstance(Configuration conf) throws IOException {
        if (singleton == null) {
            if (conf == null)
                conf = new Configuration();
            RController newController = new RController();
            newController.setConf(conf);
            newController.initialize();
            singleton = newController;
        }
        return singleton;
    };

    private static Object simplify(REXP r) {
        if (r == null)
            return null;
        int tp = r.getType();
        switch (tp) {
        case REXP.XT_STR:
            return r.asString();
        case REXP.XT_ARRAY_STR:
            return r.asStringArray();

            // TODO: ...

        default:
            return r;
        }
    }

}
