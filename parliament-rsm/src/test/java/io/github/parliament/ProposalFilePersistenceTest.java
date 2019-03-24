//package io.github.parliament;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//import java.io.File;
//import java.nio.file.FileVisitOption;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.Comparator;
//
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//
//import io.github.parliament.files.DefaultFileService;
//import io.github.parliament.paxos.acceptor.Acceptor;
//import io.github.parliament.paxos.acceptor.LocalAcceptor;
//
//class ProposalFilePersistenceTest {
//    private ProposalFilePersistence persistence;
//    private Path path = Paths.get("./parliament");
//
//    @BeforeEach
//    void setUp() throws Exception {
//        persistence = new ProposalFilePersistence(path, new DefaultFileService());
//    }
//
//    @AfterEach
//    void tearDown() throws Exception {
//        if (Files.isDirectory(path)) {
//            Files.walk(path, FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile)
//                    .forEach(File::delete);
//        }
//    }
//
//    @Test
//    void saveAndRecoverProposal() throws Exception {
//        byte[] content = "内容".getBytes();
//        Proposal proposal = new Proposal(1, content);
//        
//        persistence.saveProposal(proposal);
//
//        Proposal rec = persistence.getProposal(1).get();
//        
//        assertEquals(1, rec.getRound());
//        assertArrayEquals(content, rec.getContent());
//    }
//
//    @Test
//    void saveAndRecoverAcceptor() throws Exception {
//        byte[] content = "内容".getBytes();
//        Proposal proposal = new Proposal(1, content);
//        persistence.saveProposal(proposal);
//        
//        Acceptor<String> acceptor = new LocalAcceptor<>();
//        acceptor.prepare("1");
//        acceptor.accept("1", content);
//        acceptor.decided("最终值".getBytes());
//        
//        persistence.saveAcceptor(1, acceptor);
//        Acceptor<String> rec = persistence.<String>getAcceptor(1);
//        assertEquals(acceptor, rec);
//    }
//}
