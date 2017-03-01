package io.taskforcex.tool;

import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;

/**
 * Created by dgt on 3/1/17.
 */
public class Configuration {

    private Object conf;

    public void addYamlResource(String file) throws FileNotFoundException {
        conf = new Yaml().load(new FileInputStream(new File(file)));
    }

    private Object getObject(String path) {
        Object confWalk = conf;
        for (String key : path.split("\\.")) {
            confWalk = ((Map) confWalk).get(key);
        }

        return confWalk;
    }

    public String getString(String path) {
        return (String) getObject(path);
    }

    public Integer getInteger(String path) {
        return (Integer) getObject(path);
    }

}
