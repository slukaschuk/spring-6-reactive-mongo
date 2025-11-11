package guru.springframework.reactivemongo.services;

import guru.springframework.reactivemongo.model.CustomerDTO;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CustomerService {
    Mono<CustomerDTO> saveCustomer(Mono<CustomerDTO> customer);

    Mono<CustomerDTO> getCustomerById(String id);

    Flux<CustomerDTO> getAllCustomers();

    Mono<CustomerDTO> updateCustomer(String id, CustomerDTO customer);

    Mono<Void> deleteCustomerById(String id);
}
