package org.codefeedr.plugins.travis

import java.time.LocalDateTime

import org.codefeedr.pipeline.PipelineItem

case class GithubPushEventItem(slug: String, last_commit_sha: String, time: LocalDateTime) extends PipelineItem