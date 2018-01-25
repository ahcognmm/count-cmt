import java.io.File

class GetListFile(var path: String) {

    def getList: List[String] = {
        val folder = new File(path)
        folder.listFiles().map(file => file.getName).toList
    }

}
