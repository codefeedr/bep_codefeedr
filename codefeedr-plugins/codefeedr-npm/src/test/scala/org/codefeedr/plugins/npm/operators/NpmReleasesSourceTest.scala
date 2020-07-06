package org.codefeedr.plugins.npm.operators

import java.util.Calendar
import org.scalatest.FunSuite

/**
 * Class to test org.tudelft.operators.NpmReleasesSource,
 * the class that turns an input stream of NPM packages into a
 * Datastream of Protocol information for that specific type of package
 *
 * @author Roald van der Heijden
 *         Date: 2019-12-01 (YYYY-MM-DD)
 */
class NpmReleasesSourceTest extends FunSuite {

  // variables used for (simulation in) testing
  val npmReleasesSource = new NpmReleasesSource(NpmSourceConfig(500, -1, 16))
  val mockedString = """["amplify-codegen-appsync-model-plugin","amplify-category-hosting","amplify-frontend-android","amplify-graphql-types-generator","amplify-frontend-javascript","amplify-app","graphql-mapping-template","amplify-frontend-ios","parser-factory","@ngasemjs/server-core","@mrf3/modularize","augmentor","midi-json-parser","eslint-config-posva","els-addon-typed-templates","validator.fn","yoshi","yoshi-template-intro","yoshi-server","yoshi-server-tools","yoshi-server-testing","yoshi-server-react","yoshi-server-client","yoshi-helpers","@vlzh/nest-typeorm-i18n","yoshi-flow-monorepo","@themost/test","yoshi-flow-legacy","yoshi-flow-editor","yoshi-flow-app","yoshi-config","yoshi-common","@let.sh/cli","testcontainers","lemon-core","eslint-config-rapida","quill-blot-formatter-extended","midi-json-parser-broker","@phnq/platform","doomisoft-controller","thirty","@applitools/visual-grid-client","@code-engine/cli","swr","@nkjmsss/bend_editor","broker-factory","skm-lit","@daostack/infra","dynamodb-recs","hydra-validator-e2e","hydra-validator","hydra-validator-analyse","hydra-validator-core","@seaneiros/react-bem","homebridge-rademacher-homepilot","jslint-configs","json-midi-encoder","library-lyj","pubs","muxu-server","postcss-lowercase-text","pimatic-smartmeter-stats","@slarys/ufe-models","json-midi-encoder-broker","react-product-fruits","eslint-config-hardcore","v-comment","json-midi-encoder-worker","react-iaux","spiel-engine","@rajzik/config-babel","@rajzik/lumos","@rajzik/config-webpack","@rajzik/config-jest","@rajzik/config-prettier","@rajzik/config-eslint","@rajzik/config-typescript","@rajzik/lumos-common","@rajzik/config-danger","@infoods/tagnames","@barba/prefetch","@angular-plugins/npm","mega-nice-form","@barba/router","jupyter-offlinenotebook","@barba/css","node-calls-python","@barba/core","json-midi-message-encoder","@ehmicky/dev-tasks","djsz3y","brain-games-liza","tk-one","react-auth-form","@ehmicky/eslint-config","@sigma-db/napi","mapir-react-component","fast-case","react-modern-calendar-datepicker","bean-parser","midi-json-parser-worker","http-proxy-tool","vue-contextmenujs","@goldfishjs/composition-api","@goldfishjs/core","@goldfishjs/plugins","@goldfishjs/route","@goldfishjs/requester","@goldfishjs/reactive","@goldfishjs/pre-build","@goldfishjs/bridge","grandjs","petri-specs","worker-factory","@azure/service-bus","app-response","@abhi18av/ramdafamily","vue-command","ngrx-signalr-core","@azure/identity","@azure/app-configuration","compilerr","vue-tournament-bracket","fe-boltzmann","jvdx","okumura-api.js","@minta/minta-error-handler","bargeron-cloverui-react","eslint-config-tidy-react","ra-data-firestore-client-ada-u","ngrx-signalr","bo-transpile-bo","verovio","@pauliescanlon/gatsby-remark-sticky-table","netmodular-ui","node-red-contrib-modbus","fantasy-content-generator","fast-unique-numbers","antd-form-mate","taupdmap","neat-rich-editor","@typetron/framework","zigmium","@enigmatis/polaris-logs","no-scroll-chains","@azure/eventhubs-checkpointstore-blob","@azure/event-hubs","@yuicer/vuepress-theme-yuicer","@yuicer/vuepress-plugin-sorted-pages","@azure/keyvault-secrets","@azure/keyvault-keys","cordova-plugin-x-socialsharing","@azure/keyvault-certificates","@aomi/wbs","ss-react","@azure/cosmos","json-schema-typed","@azure/storage-queue","@azure/storage-file-datalake","@azure/storage-file-share","@azure/storage-blob","lesh-test-module","publish-svelte","persistent-programming-redis-state","authoring-library-example","@yarnaimo/next-config","vue-autocompletion","jstransformer-lowlight","@vandmo/fake-smtp-server","react-native-dynamic-bundle-loader","@lxjx/react-render-api","@cafebazaar/hod","react-draft-wysiwyg","@yaffle/expression","webpanel-admin","typed-firestore","@gamiphy/service-core","pkg","json-form-data","node-cache","tickplate","infinidesk-portal-lib","node-typescript-template","laravel-vue-form-validator","party-parrots","draftjs-utils","lior-ehrlich-casino-clover-ui","cloudwatch-front-logger","clover-ui-yael","@vitaba/common-ui","lve","@thalesrc/hermes","@binary-constructions/semantic-map","whfp-motion-webcam","@omni-door/cli","eunice","@ant-design/codemod-v4","@react-ui-org/react-ui","colineteam-binarystream","@travi/scaffolder-sub-command","timing-provider"]"""

  test("retrieving the list from the updatestream works correctly ") {
    // Act
    val jsonString = npmReleasesSource.retrieveUpdateStringFrom(npmReleasesSource.url_updatestream)
    // Assert
    assert(jsonString.get.isInstanceOf[String])
  }

// time consuming
  test("retrieve from incorrect url will fail") {
    // Arrange
    val jsonString = npmReleasesSource.retrieveUpdateStringFrom("http://www.idontexisturl.com")
    // Assert
    assert(jsonString.isEmpty)
  }

  // now for other test I'll use the mockedString
  test("parsing a string with a normal list of packages works correctly") {
    // Arrange
    val now = Calendar.getInstance.getTime
    // Act
    val listReleases = npmReleasesSource.createListOfUpdatedNpmIdsFrom(mockedString, now)
    // Assert
    assert(listReleases.size == 201)
    assert(listReleases.head.name == "amplify-codegen-appsync-model-plugin")
  }

  test(" parsing an empty string will result in an empty list of Npm releases") {
    // Arrange
    val emptyString = ""
    val now = Calendar.getInstance.getTime
    // Act
    val listReleases = npmReleasesSource.createListOfUpdatedNpmIdsFrom(emptyString, now)
    // Assert
    assert(listReleases.isEmpty)
    assert(listReleases == List())
  }

  test("waiting for a PollingInterval takes equal to or more than 1/2 second") {
    val startTime = System.currentTimeMillis()
    npmReleasesSource.waitPollingInterval()
    npmReleasesSource.waitPollingInterval()
    val endTime = System.currentTimeMillis()
    assert(endTime - startTime >= 2 * 500)
  }

  test("default values for the source config should be set") {
    val config = NpmSourceConfig()
    assert(config.pollingInterval == 10000)
    assert(config.maxNumberOfRuns == -1)
  }

  test("boilerplate test") {
    npmReleasesSource.cancel()
    assert(!npmReleasesSource.getIsRunning)
  }
}