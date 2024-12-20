/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.raft;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class FollowerStateWithVoteTest {
    private final MockTime time = new MockTime();
    private final LogContext logContext = new LogContext();
    private final int epoch = 5;
    private final int leaderId = 1;
    private final Endpoints leaderEndpoints = Endpoints.fromInetSocketAddresses(
        Collections.singletonMap(
            ListenerName.normalised("CONTROLLER"),
            InetSocketAddress.createUnresolved("mock-host-1", 1234)
        )
    );
    private final ReplicaKey votedKey = ReplicaKey.of(2, Uuid.randomUuid());
    private final Set<Integer> voters = Set.of(1, 2, 3);
    private final int fetchTimeoutMs = 15000;

    private FollowerState newFollowerVotedState() {
        return new FollowerState(
            time,
            epoch,
            leaderId,
            leaderEndpoints,
            Optional.of(votedKey),
            voters,
            Optional.empty(),
            fetchTimeoutMs,
            logContext
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPreVoteIfHasNotFetchedFromLeaderYet(boolean isLogUpToDate) {
        FollowerState state = newFollowerVotedState();
        assertEquals(isLogUpToDate, state.canGrantVote(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true));
        assertEquals(isLogUpToDate, state.canGrantVote(ReplicaKey.of(2, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true));
        assertEquals(isLogUpToDate, state.canGrantVote(ReplicaKey.of(3, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true));
        assertEquals(isLogUpToDate, state.canGrantVote(ReplicaKey.of(10, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGrantVoteAfterSuccessfulFetchFromLeader(boolean isLogUpToDate) {
        FollowerState state = newFollowerVotedState();
        state.resetFetchTimeoutForSuccessfulFetch(time.milliseconds());

        assertFalse(state.canGrantVote(ReplicaKey.of(1, Uuid.randomUuid()), isLogUpToDate, true));
        assertFalse(state.canGrantVote(ReplicaKey.of(2, Uuid.randomUuid()), isLogUpToDate, true));
        assertFalse(state.canGrantVote(ReplicaKey.of(3, Uuid.randomUuid()), isLogUpToDate, true));
        assertFalse(state.canGrantVote(ReplicaKey.of(10, Uuid.randomUuid()), isLogUpToDate, true));

        assertFalse(state.canGrantVote(ReplicaKey.of(1, Uuid.randomUuid()), isLogUpToDate, false));
        assertFalse(state.canGrantVote(ReplicaKey.of(2, Uuid.randomUuid()), isLogUpToDate, false));
        assertFalse(state.canGrantVote(ReplicaKey.of(3, Uuid.randomUuid()), isLogUpToDate, false));
        assertFalse(state.canGrantVote(ReplicaKey.of(10, Uuid.randomUuid()), isLogUpToDate, false));
    }
}
