package org.omarket.trading;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

public abstract class ContractFilter {

    private Path filename;
    private Path primaryExchange;
    private Path currency;
    private Path securityType;

    protected String prepare(Path path) throws IOException {
        int count = path.getNameCount();
        setFilename(path.getFileName());
        setPrimaryExchange(path.getName(count - 3));
        setCurrency(path.getName(count - 4));
        setSecurityType(path.getName(count - 5));
        return Files.lines(path, StandardCharsets.UTF_8).collect(Collectors.joining());
    }

    public String getFilename() {
        return filename.toString();
    }

    public String getPrimaryExchange() {
        return primaryExchange.toString();
    }

    public String getCurrency() {
        return currency.toString();
    }

    public String getSecurityType() {
        return securityType.toString();
    }

    protected abstract boolean accept(String content);

    public void setFilename(Path filename) {
        this.filename = filename;
    }

    public void setPrimaryExchange(Path primaryExchange) {
        this.primaryExchange = primaryExchange;
    }

    public void setCurrency(Path currency) {
        this.currency = currency;
    }

    public void setSecurityType(Path securityType) {
        this.securityType = securityType;
    }
}
