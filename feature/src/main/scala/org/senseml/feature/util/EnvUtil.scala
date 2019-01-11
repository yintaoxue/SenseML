package org.senseml.feature.util

import java.io.File

/**
  * EnvUtil
  * Created by xueyintao on 2019-01-11.
  */
object EnvUtil {

  /**
    * get the absolute path of current project, ends with file separator
    * @return
    */
  def getProjectPath(): String = {
    new File("").getAbsolutePath + java.io.File.separator
  }

}
