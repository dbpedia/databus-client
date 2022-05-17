package org.dbpedia.databus.client.filehandling.download

import better.files.File

class DownloadConfig(var downloadURL:String, var dataidURL:String, var sha:String, publisher:String, group:String, artifact:String, version:String, fileName:String) {

  def getOutputFile(targetDir:File):File = {
    targetDir / publisher / group / artifact / version / fileName
  }
}
