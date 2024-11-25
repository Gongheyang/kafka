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
package org.apache.kafka.server.common;

import org.apache.kafka.common.feature.SupportedVersionRange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.server.common.UnitTestFeatureVersion.UT_FV0_0;
import static org.apache.kafka.server.common.UnitTestFeatureVersion.UT_FV0_1;
import static org.apache.kafka.server.common.UnitTestFeatureVersion.UT_FV1_0;
import static org.apache.kafka.server.common.UnitTestFeatureVersion.UT_FV1_1;
import static org.apache.kafka.server.common.UnitTestFeatureVersion.UT_FV2_0;
import static org.apache.kafka.server.common.UnitTestFeatureVersion.UT_FV2_1;
import static org.apache.kafka.server.common.UnitTestFeatureVersion.UT_FV3_0;
import static org.apache.kafka.server.common.UnitTestFeatureVersion.UT_FV3_1;
import static org.apache.kafka.server.common.UnitTestFeatureVersion.UT_FV4_0;
import static org.apache.kafka.server.common.UnitTestFeatureVersion.UT_FV4_1;
import static org.apache.kafka.server.common.UnitTestFeatureVersion.UT_FV5_0;
import static org.apache.kafka.server.common.UnitTestFeatureVersion.UT_FV5_1;

/**
 * This is enum for the various features implemented for Kafka clusters.
 * KIP-584: Versioning Scheme for Features introduced the idea of various features, but only added one feature -- MetadataVersion.
 * KIP-1022: Formatting and Updating Features allowed for more features to be added. In order to set and update features,
 * they need to be specified via the StorageTool or FeatureCommand tools.
 * <br>
 * Having a unified enum for the features that will use a shared type in the API used to set and update them
 * makes it easier to process these features.
 */
public enum Features {

    /**
     * Features defined. If a feature is included in this list, and marked to be used in production they will also be specified when
     * formatting a cluster via the StorageTool. MetadataVersion is handled separately, so it is not included here.
     *
     * See {@link TestFeatureVersion} as an example. See {@link FeatureVersion} when implementing a new feature.
     */
    TEST_VERSION("test.feature.version", TestFeatureVersion.values(), TestFeatureVersion.LATEST_PRODUCTION),
    KRAFT_VERSION("kraft.version", KRaftVersion.values(), KRaftVersion.LATEST_PRODUCTION),
    TRANSACTION_VERSION("transaction.version", TransactionVersion.values(), TransactionVersion.LATEST_PRODUCTION),
    GROUP_VERSION("group.version", GroupVersion.values(), GroupVersion.LATEST_PRODUCTION),

    /**
     * Features defined only for unit tests and are not used in production.
     */
    UNIT_TEST_VERSION_0(UnitTestFeatureVersion.FEATURE_NAME + ".0", new FeatureVersion[]{UT_FV0_0}, UT_FV0_1),
    UNIT_TEST_VERSION_1(UnitTestFeatureVersion.FEATURE_NAME + ".1", new FeatureVersion[]{UT_FV1_0, UT_FV1_1}, UT_FV1_0),
    UNIT_TEST_VERSION_2(UnitTestFeatureVersion.FEATURE_NAME + ".2", new FeatureVersion[]{UT_FV2_0, UT_FV2_1}, UT_FV2_0),
    UNIT_TEST_VERSION_3(UnitTestFeatureVersion.FEATURE_NAME + ".3", new FeatureVersion[]{UT_FV3_0, UT_FV3_1}, UT_FV3_1),
    UNIT_TEST_VERSION_4(UnitTestFeatureVersion.FEATURE_NAME + ".4", new FeatureVersion[]{UT_FV4_0, UT_FV4_1}, UT_FV4_1),
    UNIT_TEST_VERSION_5(UnitTestFeatureVersion.FEATURE_NAME + ".5", new FeatureVersion[]{UT_FV5_0, UT_FV5_1}, UT_FV5_1);

    public static final Features[] FEATURES;
    public static final List<Features> PRODUCTION_FEATURES;

    public static final List<String> PRODUCTION_FEATURE_NAMES;
    private final String name;
    private final FeatureVersion[] featureVersions;

    // The latest production version of the feature, owned and updated by the feature owner
    // in the respective feature definition. The value should not be smaller than the default
    // value calculated with {@link #defaultValue(MetadataVersion)}.
    public final FeatureVersion latestProduction;

    private static final Logger log = LoggerFactory.getLogger(Features.class);

    Features(String name,
             FeatureVersion[] featureVersions,
             FeatureVersion latestProduction) {
        this.name = name;
        this.featureVersions = featureVersions;
        this.latestProduction = latestProduction;
    }

    static {
        Features[] enumValues = Features.values();
        FEATURES = Arrays.copyOf(enumValues, enumValues.length);

        PRODUCTION_FEATURES = Arrays.stream(FEATURES).filter(feature ->
            !feature.name.equals(TEST_VERSION.featureName()) &&
            !feature.name.startsWith(UnitTestFeatureVersion.FEATURE_NAME)
        ).collect(Collectors.toList());
        PRODUCTION_FEATURE_NAMES = PRODUCTION_FEATURES.stream().map(feature ->
                feature.name).collect(Collectors.toList());

        validateDefaultValueAndLatestProductionValue(TEST_VERSION);
        for (Features feature : PRODUCTION_FEATURES) {
            validateDefaultValueAndLatestProductionValue(feature);
        }
    }

    public String featureName() {
        return name;
    }

    public FeatureVersion[] featureVersions() {
        return featureVersions;
    }

    public short latestProduction() {
        return latestProduction.featureLevel();
    }

    public short minimumProduction() {
        return featureVersions[0].featureLevel();
    }

    public short latestTesting() {
        return featureVersions[featureVersions.length - 1].featureLevel();
    }

    public SupportedVersionRange supportedVersionRange() {
        return new SupportedVersionRange(
            minimumProduction(),
            latestTesting()
        );
    }

    /**
     * Creates a FeatureVersion from a level.
     *
     * @param level                        the level of the feature
     * @param allowUnstableFeatureVersions whether unstable versions can be used
     * @return the FeatureVersionUtils.FeatureVersion for the feature the enum is based on.
     * @throws IllegalArgumentException    if the feature is not known.
     */
    public FeatureVersion fromFeatureLevel(short level,
                                           boolean allowUnstableFeatureVersions) {
        return Arrays.stream(featureVersions).filter(featureVersion ->
            featureVersion.featureLevel() == level && (allowUnstableFeatureVersions || level <= latestProduction())).findFirst().orElseThrow(
                () -> new IllegalArgumentException("No feature:" + featureName() + " with feature level " + level));
    }

    /**
     * A method to validate the feature can be set. If a given feature relies on another feature, the dependencies should be
     * captured in {@link FeatureVersion#dependencies()}
     * <p>
     * For example, say feature X level x relies on feature Y level y:
     * if feature X >= x then throw an error if feature Y < y.
     *
     * All feature levels above 0 in kraft require metadata.version=4 (IBP_3_3_IV0) in order to write the feature records to the cluster.
     *
     * @param feature                   the feature we are validating
     * @param features                  the feature versions we have (or want to set)
     * @throws IllegalArgumentException if the feature is not valid
     */
    public static void validateVersion(FeatureVersion feature, Map<String, Short> features) {
        Short metadataVersion = features.get(MetadataVersion.FEATURE_NAME);

        if (feature.featureLevel() >= 1 && (metadataVersion == null || metadataVersion < MetadataVersion.IBP_3_3_IV0.featureLevel()))
            throw new IllegalArgumentException(feature.featureName() + " could not be set to " + feature.featureLevel() +
                    " because it depends on metadata.version=4 (" + MetadataVersion.IBP_3_3_IV0 + ")");

        for (Map.Entry<String, Short> dependency: feature.dependencies().entrySet()) {
            Short featureLevel = features.get(dependency.getKey());

            if (featureLevel == null || featureLevel < dependency.getValue()) {
                throw new IllegalArgumentException(feature.featureName() + " could not be set to " + feature.featureLevel() +
                        " because it depends on " + dependency.getKey() + " level " + dependency.getValue());
            }
        }
    }

    /**
     * A method to return the default (latest production) version of a feature based on the metadata version provided.
     *
     * Every time a new feature is added, it should create a mapping from metadata version to feature version
     * with {@link FeatureVersion#bootstrapMetadataVersion()}. The feature version should be marked as production ready
     * before the metadata version is made production ready.
     *
     * @param metadataVersion the metadata version we want to use to set the default.
     * @return the default version given the feature and provided metadata version
     */
    public FeatureVersion defaultVersion(MetadataVersion metadataVersion) {
        FeatureVersion version = featureVersions[0];
        for (Iterator<FeatureVersion> it = Arrays.stream(featureVersions).iterator(); it.hasNext(); ) {
            FeatureVersion feature = it.next();
            if (feature.bootstrapMetadataVersion().isLessThan(metadataVersion) || feature.bootstrapMetadataVersion().equals(metadataVersion))
                version = feature;
            else
                return version;
        }
        return version;
    }

    public short defaultLevel(MetadataVersion metadataVersion) {
        return defaultVersion(metadataVersion).featureLevel();
    }

    public static Features featureFromName(String featureName) {
        for (Features features : FEATURES) {
            if (features.name.equals(featureName))
                return features;
        }
        throw new IllegalArgumentException("Feature " + featureName + " not found.");
    }

    /**
     * Utility method to map a list of FeatureVersion to a map of feature name to feature level
     */
    public static Map<String, Short> featureImplsToMap(List<FeatureVersion> features) {
        return features.stream().collect(Collectors.toMap(FeatureVersion::featureName, FeatureVersion::featureLevel));
    }

    public boolean isProductionReady(short featureVersion) {
        return featureVersion <= latestProduction();
    }

    public boolean hasFeatureVersion(FeatureVersion featureVersion) {
        for (FeatureVersion v : featureVersions()) {
            if (v == featureVersion) {
                return true;
            }
        }
        return false;
    }

    /**
     * The method ensures that the following statements are met:
     * 1. The latest production value is one of the feature values.
     * 2. The latest production value >= the default value.
     * 3. The dependencies of the latest production value <= their latest production values.
     * 4. The dependencies of the default value <= their default values.
     *
     * Suppose we have feature X as the feature being validated.
     * Invalid examples:
     *     - The feature X has default version = XV_10 (dependency = {}), latest production = XV_5 (dependency = {})
     *       (Violating rule 2. The latest production value XV_5 is smaller than the default value)
     *     - The feature X has default version = XV_10 (dependency = {Y: YV_3}), latest production = XV_11 (dependency = {Y: YV_4})
     *       The feature Y has default version = YV_3 (dependency = {}), latest production = YV_3 (dependency = {})
     *       (Violating rule 3. For latest production XV_11, Y's latest production YV_3 is smaller than the dependency value YV_4)
     *     - The feature X has default version = XV_10 (dependency = {Y: YV_4}), latest production = XV_11 (dependency = {Y: YV_4})
     *       The feature Y has default version = YV_3 (dependency = {}), latest production = YV_4 (dependency = {})
     *       (Violating rule 4. For default version XV_10, Y's default value YV_3 is smaller than the dependency value YV_4)
     * Valid examples:
     *     - The feature X has default version = XV_10 (dependency = {}), latest production = XV_10 (dependency = {})
     *     - The feature X has default version = XV_10 (dependency = {Y: YV_3}), latest production = XV_11 (dependency = {Y: YV_4})
     *       The feature Y has default version = YV_3 (dependency = {}), latest production = YV_4 (dependency = {})
     *     - The feature X has default version = latest production = XV_10 (dependency = {MetadataVersion: IBP_4_0_IV0})
     *       (Some features can depend on MetadataVersion, which is not checked in this method)
     *
     * @param features the feature to validate.
     * @return true if the feature is valid, false otherwise.
     * @throws IllegalArgumentException if the feature violates any of the rules thus is not valid.
     */
    public static void validateDefaultValueAndLatestProductionValue(
        Features features
    ) throws IllegalArgumentException {
        FeatureVersion defaultVersion = features.defaultVersion(MetadataVersion.LATEST_PRODUCTION);
        FeatureVersion latestProduction = features.latestProduction;

        if (!features.hasFeatureVersion(latestProduction)) {
            throw new IllegalArgumentException(String.format("Feature %s has latest production version %s=%s " +
                    "which is not one of its feature versions.",
                features.name(), latestProduction.featureName(), latestProduction.featureLevel()));
        }

        if (latestProduction.featureLevel() < defaultVersion.featureLevel()) {
            throw new IllegalArgumentException(String.format("Feature %s has latest production value %s " +
                    "smaller than its default value %s.",
                features.name(), latestProduction.featureLevel(), defaultVersion.featureLevel()));
        }

        for (Map.Entry<String, Short> dependency: latestProduction.dependencies().entrySet()) {
            String dependencyFeatureName = dependency.getKey();
            // There might be dependency on MetadataVersion which is not a feature, so we skip checking it.
            if (!dependencyFeatureName.equals(MetadataVersion.FEATURE_NAME)) {
                Features dependencyFeature = featureFromName(dependencyFeatureName);
                if (!dependencyFeature.isProductionReady(dependency.getValue())) {
                    throw new IllegalArgumentException(String.format("Latest production FeatureVersion %s=%s " +
                            "has a dependency %s=%s that is not production ready. (%s latest production: %s)",
                        features.name(), latestProduction.featureLevel(), dependencyFeature.name(), dependency.getValue(),
                        dependencyFeature.name(), dependencyFeature.latestProduction()));
                }
            }
        }

        for (Map.Entry<String, Short> dependency: defaultVersion.dependencies().entrySet()) {
            String dependencyFeatureName = dependency.getKey();
            // There might be dependency on MetadataVersion which is not a feature, so we skip checking it.
            if (!dependencyFeatureName.equals(MetadataVersion.FEATURE_NAME)) {
                Features dependencyFeature = featureFromName(dependencyFeatureName);
                if (dependency.getValue() > dependencyFeature.defaultLevel(MetadataVersion.LATEST_PRODUCTION)) {
                    throw new IllegalArgumentException(String.format("Default FeatureVersion %s=%s has " +
                            "a dependency %s=%s that is ahead of its default value %s.",
                        features.name(), defaultVersion.featureLevel(), dependencyFeature.name(), dependency.getValue(),
                        dependencyFeature.defaultLevel(MetadataVersion.LATEST_PRODUCTION)));
                }
            }
        }
    }
}
