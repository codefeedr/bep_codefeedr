/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.codefeedr.plugins.cratesio

import java.util.Date
import java.util.Locale.Category

/**
  * Classes for the crates.io response schema.
  */
object CargoProtocol {

  case class CrateLinks(version_downloads: String,
                        versions: String,
                        owners: String,
                        owner_team: String,
                        owner_user: String,
                        reverse_dependencies: String)

  case class BadgeAttribute(id: Option[String],
                            repository: Option[String],
                            branch: Option[String],
                            project_name: Option[String],
                            service: Option[String])

  case class Badge(badge_type: String,
                   attributes: BadgeAttribute)

  case class Crate(id: String,
                   name: String,
                   updated_at: String,
                   versions: List[Int],
                   keywords: List[String],
                   categories: List[String],
                   badges: List[Badge],
                   created_at: String,
                   downloads: Double,
                   recent_downloads: Double,
                   max_version: String,
                   description: String,
                   homepage: Option[String],
                   documentation: Option[String],
                   repository: Option[String],
                   links: CrateLinks,
                   exact_match: Boolean)

  case class Features(default: List[String],
                      use_std: List[Double])

  case class VersionLinks(dependencies: String,
                          version_downloads: String,
                          authors: String)

  case class Version(id: Double,
                     crate: String,
                     num: String,
                     dl_path: String,
                     readme_path: String,
                     updated_at: String,
                     created_at: String,
                     downloads: Double,
                     features: Features,
                     yanked: Boolean,
                     license: String,
                     links: VersionLinks)

  case class Keyword(id: String,
                     keyword: String,
                     created_at: String,
                     crates_cnt: Double)

  case class Category(id: String,
                       category: String,
                       slug: String,
                       description: String,
                       created_at: String,
                       crates_cnt: Double)

  case class CrateInfo(crate: Crate,
                       versions: List[Version],
                       keywords: List[Keyword],
                       categories: List[Category])
}
