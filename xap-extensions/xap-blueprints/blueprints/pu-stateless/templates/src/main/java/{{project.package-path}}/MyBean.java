/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package {{project.groupId}};

import org.slf4j.*;
import javax.annotation.*;

import org.openspaces.core.*;
import org.openspaces.core.space.*;

public class MyBean {
    private static final Logger logger = LoggerFactory.getLogger(MyBean.class);

    @Resource
    private GigaSpace gigaSpace;

    @PostConstruct
    public void initialize() {
        logger.info("Initialized: connected to space {}", gigaSpace.getSpaceName());
        // Your code goes here, for example:
        int count = gigaSpace.count(null);
        logger.info("Entries in space: {}", count);
    }

    @PreDestroy
    public void close() {
        logger.info("Closing");
    }
}
