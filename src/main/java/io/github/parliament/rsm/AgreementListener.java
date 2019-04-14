package io.github.parliament.rsm;

/**
 *
 * @author zy
 */
interface AgreementListener {
    void notice(RoundLocalAcceptor acceptor) throws Exception;
}