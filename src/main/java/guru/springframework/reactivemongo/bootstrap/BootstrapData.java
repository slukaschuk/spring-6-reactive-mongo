package guru.springframework.reactivemongo.bootstrap;

import guru.springframework.reactivemongo.domain.Beer;
import guru.springframework.reactivemongo.domain.Customer;
import guru.springframework.reactivemongo.repositories.BeerRepository;
import guru.springframework.reactivemongo.repositories.CustomerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Created by jt, Spring Framework Guru.
 */
@Component
@RequiredArgsConstructor
public class BootstrapData implements CommandLineRunner {

    private final BeerRepository beerRepository;
    private final CustomerRepository customerRepository;

    @Override
    public void run(String... args) throws Exception {
        beerRepository.deleteAll()
                .doOnSuccess(success -> {
                    loadBeerData();
                })
                .subscribe();

        customerRepository.deleteAll()
                .then(loadCustomerData())
                .subscribe();
    }

    private void loadBeerData() {

        beerRepository.count().subscribe(count -> {
            if (count == 0) {
                Beer beer1 = Beer.builder()
                        .beerName("Galaxy Cat")
                        .beerStyle("Pale Ale")
                        .upc("12356")
                        .price(new BigDecimal("12.99"))
                        .quantityOnHand(122)
                        .createdDate(LocalDateTime.now())
                        .lastModifiedDate(LocalDateTime.now())
                        .build();

                Beer beer2 = Beer.builder()
                        .beerName("Crank")
                        .beerStyle("Pale Ale")
                        .upc("12356222")
                        .price(new BigDecimal("11.99"))
                        .quantityOnHand(392)
                        .createdDate(LocalDateTime.now())
                        .lastModifiedDate(LocalDateTime.now())
                        .build();

                Beer beer3 = Beer.builder()
                        .beerName("Sunshine City")
                        .beerStyle("IPA")
                        .upc("12356")
                        .price(new BigDecimal("13.99"))
                        .quantityOnHand(144)
                        .createdDate(LocalDateTime.now())
                        .lastModifiedDate(LocalDateTime.now())
                        .build();

                beerRepository.save(beer1).subscribe(beer -> {
                    System.out.println(beer.toString());
                });
                beerRepository.save(beer2).subscribe(beer -> {
                    System.out.println(beer.toString());
                });
                beerRepository.save(beer3).subscribe(beer -> {
                    System.out.println(beer.toString());
                });

                System.out.println("Loaded Beers: " + beerRepository.count().block());
            }
        });
    }

    private Mono<Void> loadCustomerData() {
        return customerRepository.count()
                .filter(count -> count == 0)
                .flatMapMany(ignored -> Flux.just(
                        Customer.builder().customerName("Serhii")
                                .createdDate(LocalDateTime.now())
                                .lastModifiedDate(LocalDateTime.now()).build(),
                        Customer.builder().customerName("Inna")
                                .createdDate(LocalDateTime.now())
                                .lastModifiedDate(LocalDateTime.now()).build()
                        ))
                .flatMap(customerRepository::save)
                .then();
    }
}
