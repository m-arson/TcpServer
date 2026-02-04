import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

public class TcpServer {
    private final Properties config = new Properties();
    private ExecutorService workerPool;
    private final Map<SocketChannel, StringBuilder> sessionStates = new ConcurrentHashMap<>();
    private final Map<SocketChannel, Long> lastActive = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleaner = Executors.newSingleThreadScheduledExecutor();
    private Selector selector;
    private ServerSocketChannel serverChannel;

    public TcpServer() {
        loadConfig();
        validateMandatory();
        initializeThreadPool();
        setupShutdownHook();
    }

    private void loadConfig() {
        try {
            if (Files.exists(Paths.get(".env"))) {
                List<String> lines = Files.readAllLines(Paths.get(".env"));
                for (String line : lines) {
                    line = line.trim();
                    if (line.isEmpty() || line.startsWith("#")) continue;
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        config.put(parts[0].trim(), parts[1].trim());
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Warning: Error reading .env file. Proceeding with defaults/mandatory check.");
        }
    }

    private void validateMandatory() {
        if (!config.containsKey("PORT") || config.getProperty("PORT").trim().isEmpty()) {
            throw new RuntimeException("Fatal Error: PORT is required in .env but was not found.");
        }
    }

    private String getOpt(String key, String defaultValue) {
        return config.getProperty(key, defaultValue);
    }

    private void initializeThreadPool() {
        int core = Integer.parseInt(getOpt("CORE_POOL_SIZE", "10"));
        int max = Integer.parseInt(getOpt("MAX_POOL_SIZE", "20"));
        long keepAlive = Long.parseLong(getOpt("KEEP_ALIVE_TIME_SECONDS", "60"));
        int queueCap = Integer.parseInt(getOpt("QUEUE_CAPACITY", "1000"));

        this.workerPool = new ThreadPoolExecutor(
                core, max, keepAlive, TimeUnit.SECONDS, 
                new ArrayBlockingQueue<>(queueCap)
        );
    }

    private void setupShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                cleaner.shutdownNow();
                workerPool.shutdown();
                if (selector != null) selector.close();
                if (serverChannel != null) serverChannel.close();
                for (SocketChannel client : sessionStates.keySet()) client.close();
            } catch (IOException e) {
                System.err.println("Shutdown error: " + e.getMessage());
            }
        }));
    }

    public void start() throws IOException {
        int port = Integer.parseInt(config.getProperty("PORT"));
        String host = getOpt("HOST", "0.0.0.0");
        long cleanupInterval = Long.parseLong(getOpt("CLEANUP_INTERVAL_MS", "30000"));

        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(host, port));
        serverChannel.configureBlocking(false);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        cleaner.scheduleAtFixedRate(this::cleanupIdleConnections, 
                cleanupInterval, cleanupInterval, TimeUnit.MILLISECONDS);

        System.out.println("Server started on " + host + ":" + port);

        while (!Thread.currentThread().isInterrupted()) {
            if (selector.select() == 0) continue;
            
            Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
            while (iter.hasNext()) {
                SelectionKey key = iter.next();
                iter.remove();
                if (!key.isValid()) continue;

                if (key.isAcceptable()) {
                    handleAccept(serverChannel, selector);
                } else if (key.isReadable()) {
                    key.interestOps(0); 
                    handleRead(key);
                }
            }
        }
    }

    private void handleAccept(ServerSocketChannel serverChannel, Selector selector) throws IOException {
        SocketChannel client = serverChannel.accept();
        client.configureBlocking(false);
        client.register(selector, SelectionKey.OP_READ);
        sessionStates.put(client, new StringBuilder());
        lastActive.put(client, System.currentTimeMillis());
    }

    private void handleRead(SelectionKey key) {
        workerPool.submit(() -> {
            SocketChannel client = (SocketChannel) key.channel();
            ByteBuffer buffer = ByteBuffer.allocate(2048);
            try {
                int bytesRead = client.read(buffer);
                if (bytesRead == -1) {
                    closeConnection(client);
                    return;
                }
                lastActive.put(client, System.currentTimeMillis());
                buffer.flip();
                byte[] data = new byte[buffer.remaining()];
                buffer.get(data);

                StringBuilder sb = sessionStates.get(client);
                if (sb != null) {
                    sb.append(new String(data));
                    String content = sb.toString();
                    if (content.contains("\n")) {
                        String[] messages = content.split("\n", -1);
                        for (int i = 0; i < messages.length - 1; i++) {
                            processBusinessLogic(client, messages[i]);
                        }
                        sb.setLength(0);
                        sb.append(messages[messages.length - 1]);
                    }
                }
                if (key.isValid()) {
                    key.interestOps(SelectionKey.OP_READ);
                    key.selector().wakeup();
                }
            } catch (IOException e) {
                closeConnection(client);
            }
        });
    }

    private void processBusinessLogic(SocketChannel client, String message) {
        try {
            String response = "OK: " + message.trim() + "\n";
            client.write(ByteBuffer.wrap(response.getBytes()));
        } catch (IOException e) {
            closeConnection(client);
        }
    }

    private void cleanupIdleConnections() {
        long now = System.currentTimeMillis();
        long timeout = Long.parseLong(getOpt("IDLE_TIMEOUT_MS", "600000"));
        lastActive.forEach((client, time) -> {
            if (now - time > timeout) closeConnection(client);
        });
    }

    private void closeConnection(SocketChannel client) {
        try {
            sessionStates.remove(client);
            lastActive.remove(client);
            client.close();
        } catch (IOException ignored) {}
    }

    public static void main(String[] args) throws IOException {
        new TcpServer().start();
    }
}
