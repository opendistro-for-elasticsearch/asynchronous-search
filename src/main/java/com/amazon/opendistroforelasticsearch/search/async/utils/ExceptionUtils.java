package com.amazon.opendistroforelasticsearch.search.async.utils;

import java.util.Locale;

public class ExceptionUtils {

    public static String getRnfMessageForDelete(String id) {
        return String.format(Locale.ROOT, "Attempting to delete non-existent asynchronous search [%s]", id);
    }

    public static String getRnfMessageForGet(String id) {
        return String.format(Locale.ROOT, "Unable to find asynchronous search [%s]", id);
    }

}
