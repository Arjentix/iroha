/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef FAKE_PEER_ODOS_NETWORK_NOTIFIER_HPP_
#define FAKE_PEER_ODOS_NETWORK_NOTIFIER_HPP_

#include <mutex>
#include <rxcpp/rx-lite.hpp>

#include "consensus/round.hpp"
#include "framework/integration_framework/fake_peer/types.hpp"
#include "ordering/on_demand_ordering_service.hpp"

namespace integration_framework::fake_peer {

  class OnDemandOsNetworkNotifier final
      : public iroha::ordering::OnDemandOrderingService {
   public:
    OnDemandOsNetworkNotifier(const std::shared_ptr<FakePeer> &fake_peer);

    void onBatches(CollectionType batches) override;

    iroha::ordering::ProposalWithHash onRequestProposal(
        iroha::consensus::Round const &) override;

    void onCollaborationOutcome(iroha::consensus::Round round) override;

    void onTxsCommitted(const HashesSetType &hashes) override;

    void onDuplicates(const HashesSetType &hashes) override;

    void forCachedBatches(
        std::function<void(
            iroha::ordering::OnDemandOrderingService::BatchesSetType &)> const
            &f) override;

    bool isEmptyBatchesCache() override;

    bool hasEnoughBatchesInCache() const override;

    bool hasProposal(iroha::consensus::Round round) const override;

    void processReceivedProposal(CollectionType batches) override;

    rxcpp::observable<iroha::consensus::Round> getProposalRequestsObservable();

    rxcpp::observable<std::shared_ptr<BatchesCollection>>
    getBatchesObservable();

    shared_model::crypto::Hash getProposalHash(
        iroha::consensus::Round round) override {
      return {};
    }

    iroha::ordering::ProposalWithHash getProposalWithHash(
        iroha::consensus::Round round) override {
      return {};
    }

   private:
    std::weak_ptr<FakePeer> fake_peer_wptr_;
    rxcpp::subjects::subject<iroha::consensus::Round> rounds_subject_;
    std::mutex rounds_subject_mutex_;
    rxcpp::subjects::subject<std::shared_ptr<BatchesCollection>>
        batches_subject_;
    std::mutex batches_subject_mutex_;
  };

}  // namespace integration_framework::fake_peer

#endif /* FAKE_PEER_ODOS_NETWORK_NOTIFIER_HPP_ */
