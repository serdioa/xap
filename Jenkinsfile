node('gspaces-builder') {
    properties([
        disableConcurrentBuilds(),
        buildDiscarder(
            logRotator(
                artifactNumToKeepStr: "7",
                numToKeepStr: "7"
            )
        )
    ])
    @Library('xap-common@preversion')_

    stage('BUILD') {
        xap.xapBuild();
    }

    stage('TEST') {
        xap.xapTest();
    }

    stage('DEPLOY') {
        xap.xapDeploy();
    }
}
