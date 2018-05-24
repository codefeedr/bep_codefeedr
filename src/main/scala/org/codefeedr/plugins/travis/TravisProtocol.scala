package org.codefeedr.plugins.travis

import java.time.LocalDateTime

import org.codefeedr.pipeline.PipelineItem
import org.codefeedr.plugins.github.GitHubProtocol.PushEvent

object TravisProtocol {

  case class PushEventFromActiveTravisRepo(pushEventItem: PushEvent) extends PipelineItem

  case class TravisBuilds(`@type`: String,
                          `@href`: String,
                          `@representation`: String,
                          `@pagination`: TravisPagination,
                          builds: List[TravisBuild])

  case class TravisBuild(`@type`: String,
                         `@href`: String,
                         `@representation`: String,
                         `@permissions`: TravisPermissions,
                         id: Int,
                         number: String,
                         state: String,
                         duration: Option[Int],
                         event_type: String,
                         previous_state: String,
                         pull_request_title: Option[String],
                         pull_request_number: Option[Int],
                         started_at: Option[LocalDateTime],
                         finished_at: Option[LocalDateTime],
                         `private`: Boolean,
                         repository: TravisRepository,
                         branch: TravisBranch,
                         tag: String,
                         commit: TravisCommit) extends PipelineItem

  case class TravisPermissions(read: Boolean,
                               cancel: Boolean,
                               restart: Boolean)

  case class TravisPagination(limit: Int,
                              offset: Int,
                              count: Int,
                              is_first: Boolean,
                              is_last: Boolean,
                              next: TravisPage,
                              prev: TravisPage,
                              first: TravisPage,
                              last: TravisPage)

  case class TravisPage(`@href`: String,
                        offset: Int,
                        limit: Int)

  case class TravisRepository(id: Int,
                              name: String,
                              slug: String,
                              description: Option[String],
                              github_id: Option[Int],
                              github_language: Option[String],
                              active: Option[Boolean],
                              `private`: Option[Boolean],
                              owner: Option[TravisOwner],
                              default_branch: Option[TravisBranch],
                              starred: Option[Boolean],
                              managed_by_installation: Option[Boolean],
                              active_on_org: Option[Boolean]
                             )

  case class TravisBranch(name: String,
                          repositoy: Option[TravisRepository],
                          default_branch: Option[Boolean],
                          exists_on_github: Option[Boolean],
                          last_build: Option[TravisBuild],
                          recent_builds: Option[List[TravisBuild]])

  case class TravisCommit(id: Int,
                          sha: String,
                          ref: String,
                          message: String,
                          compare_url: String,
                          committed_at: String)

  case class TravisOwner(id: Int,
                         login: String,
                         name: Option[String],
                         github_id: Option[Int],
                         avatar_url: Option[String],
                         education: Option[Boolean])
}

