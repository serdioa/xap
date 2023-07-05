package com.gigaspaces.utils;

import java.nio.file.Path;

public interface RedologFlushNotifier {
    void notifyOnFlush(String fullSpaceName, String spaceName, long redolosize, Path redologPath  );
}
