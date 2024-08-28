// Generated by jextract

package org.chdb.ffm;

import java.lang.foreign.*;
import java.lang.invoke.*;
import java.util.*;
import java.util.stream.*;

import static java.lang.foreign.ValueLayout.*;

public class LibChdb {

    LibChdb() {
        // Should not be called directly
    }

    static final Arena LIBRARY_ARENA = Arena.ofAuto();
    static final boolean TRACE_DOWNCALLS = Boolean.getBoolean("jextract.trace.downcalls");

    static void traceDowncall(String name, Object... args) {
        String traceArgs = Arrays.stream(args)
                .map(Object::toString)
                .collect(Collectors.joining(", "));
        System.out.printf("%s(%s)\n", name, traceArgs);
    }

    static MemorySegment findOrThrow(String symbol) {
        return SYMBOL_LOOKUP.find(symbol)
                .orElseThrow(() -> new UnsatisfiedLinkError("unresolved symbol: " + symbol));
    }

    static MethodHandle upcallHandle(Class<?> fi, String name, FunctionDescriptor fdesc) {
        try {
            return MethodHandles.lookup().findVirtual(fi, name, fdesc.toMethodType());
        } catch (ReflectiveOperationException ex) {
            throw new AssertionError(ex);
        }
    }

    static MemoryLayout align(MemoryLayout layout, long align) {
        return switch (layout) {
            case PaddingLayout p -> p;
            case ValueLayout v -> v.withByteAlignment(align);
            case GroupLayout g -> {
                MemoryLayout[] alignedMembers = g.memberLayouts().stream()
                        .map(m -> align(m, align)).toArray(MemoryLayout[]::new);
                yield g instanceof StructLayout ?
                        MemoryLayout.structLayout(alignedMembers) : MemoryLayout.unionLayout(alignedMembers);
            }
            case SequenceLayout s -> MemoryLayout.sequenceLayout(s.elementCount(), align(s.elementLayout(), align));
        };
    }

    static final SymbolLookup SYMBOL_LOOKUP = SymbolLookup.libraryLookup(System.mapLibraryName("chdb"), LIBRARY_ARENA)
            .or(SymbolLookup.loaderLookup())
            .or(Linker.nativeLinker().defaultLookup());

    public static final ValueLayout.OfBoolean C_BOOL = ValueLayout.JAVA_BOOLEAN;
    public static final ValueLayout.OfByte C_CHAR = ValueLayout.JAVA_BYTE;
    public static final ValueLayout.OfShort C_SHORT = ValueLayout.JAVA_SHORT;
    public static final ValueLayout.OfInt C_INT = ValueLayout.JAVA_INT;
    public static final ValueLayout.OfLong C_LONG_LONG = ValueLayout.JAVA_LONG;
    public static final ValueLayout.OfFloat C_FLOAT = ValueLayout.JAVA_FLOAT;
    public static final ValueLayout.OfDouble C_DOUBLE = ValueLayout.JAVA_DOUBLE;
    public static final AddressLayout C_POINTER = ValueLayout.ADDRESS
            .withTargetLayout(MemoryLayout.sequenceLayout(java.lang.Long.MAX_VALUE, JAVA_BYTE));
    public static final ValueLayout.OfLong C_LONG = ValueLayout.JAVA_LONG;

    private static class query_stable_v2 {
        public static final FunctionDescriptor DESC = FunctionDescriptor.of(
                LibChdb.C_POINTER,
                LibChdb.C_INT,
                LibChdb.C_POINTER
        );

        public static final MethodHandle HANDLE = Linker.nativeLinker().downcallHandle(
                LibChdb.findOrThrow("query_stable_v2"),
                DESC);
    }

    /**
     * Function descriptor for:
     * {@snippet lang = c:
     * struct local_result_v2 *query_stable_v2(int argc, char **argv)
     *}
     */
    public static FunctionDescriptor query_stable_v2$descriptor() {
        return query_stable_v2.DESC;
    }

    /**
     * Downcall method handle for:
     * {@snippet lang = c:
     * struct local_result_v2 *query_stable_v2(int argc, char **argv)
     *}
     */
    public static MethodHandle query_stable_v2$handle() {
        return query_stable_v2.HANDLE;
    }

    /**
     * {@snippet lang = c:
     * struct local_result_v2 *query_stable_v2(int argc, char **argv)
     *}
     */
    public static MemorySegment query_stable_v2(int argc, MemorySegment argv) {
        var mh$ = query_stable_v2.HANDLE;
        try {
            if (TRACE_DOWNCALLS) {
                traceDowncall("query_stable_v2", argc, argv);
            }
            return (MemorySegment) mh$.invokeExact(argc, argv);
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
    }

    private static class free_result_v2 {
        public static final FunctionDescriptor DESC = FunctionDescriptor.ofVoid(
                LibChdb.C_POINTER
        );

        public static final MethodHandle HANDLE = Linker.nativeLinker().downcallHandle(
                LibChdb.findOrThrow("free_result_v2"),
                DESC);
    }

    /**
     * Function descriptor for:
     * {@snippet lang = c:
     * void free_result_v2(struct local_result_v2 *result)
     *}
     */
    public static FunctionDescriptor free_result_v2$descriptor() {
        return free_result_v2.DESC;
    }

    /**
     * Downcall method handle for:
     * {@snippet lang = c:
     * void free_result_v2(struct local_result_v2 *result)
     *}
     */
    public static MethodHandle free_result_v2$handle() {
        return free_result_v2.HANDLE;
    }

    /**
     * {@snippet lang = c:
     * void free_result_v2(struct local_result_v2 *result)
     *}
     */
    public static void free_result_v2(MemorySegment result) {
        var mh$ = free_result_v2.HANDLE;
        try {
            if (TRACE_DOWNCALLS) {
                traceDowncall("free_result_v2", result);
            }
            mh$.invokeExact(result);
        } catch (Throwable ex$) {
            throw new AssertionError("should not reach here", ex$);
        }
    }
}

