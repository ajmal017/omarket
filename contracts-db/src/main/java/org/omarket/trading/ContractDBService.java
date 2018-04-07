package org.omarket.trading;

import rx.Observable;

import java.io.IOException;
import java.nio.file.Path;

public interface ContractDBService {
    ContractFilter FILTER_NONE = new ContractFilter() {

        @Override
        protected boolean accept(String content) {
            return true;
        }
    };

    ContractFilter filterCurrency(String currencyCode);

    ContractFilter filterExchange(String exchangeCode);

    ContractFilter filterSecurityType(String securityType);

    ContractFilter composeFilter(ContractFilter... filters);

    Security loadContract(Path contractsDirPath, String productCode) throws IOException;

    void saveContract(Path contractsDirPath, Security product) throws IOException;

    Observable<Security> loadContracts(Path contractsDirPath, ContractFilter filter) throws IOException;

}
