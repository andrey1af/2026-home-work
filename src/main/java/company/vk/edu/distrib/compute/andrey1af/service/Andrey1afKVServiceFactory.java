package company.vk.edu.distrib.compute.andrey1af.service;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.andrey1af.dao.InFileDao;

import java.io.IOException;
import java.nio.file.Path;

public class Andrey1afKVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new Andrey1afKVService(port, new InFileDao(Path.of(".data")));
    }
}
