package org.codefeedr.plugins.travis

import org.codefeedr.pipeline.PipelineItem

case class GithubPushEventItem(slug: String) extends PipelineItem